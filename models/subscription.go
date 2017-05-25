package models

import (
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Subscription struct {
	Name               string              `json:"name"`
	TopicID            string              `json:"topic"`
	Message            *MessageStatusStore `json:"-"`
	DefaultAckDeadline time.Duration       `json:"ack_deadline_seconds"`
	PushConfig         *Push               `json:"push_config"`

	// push params
	PushTick    time.Duration `json:"-"`
	AbortPush   bool          `json:"-"`
	PushRunning bool          `json:"-"`
	PushSize    int           `json:"-"`
	abortMu     sync.RWMutex  `json:"-"`
	runningMu   sync.RWMutex  `json:"-"`
	sizeMu      sync.RWMutex  `json:"-"`
}

const (
	// push variables
	PushInterval = 10 * time.Second
	MaxPushSize  = 1000
	MinPushSize  = 1
)

// Create Subscription, if not exist already same name Subscription
func NewSubscription(name, topicName string, timeout int64, endpoint string, attr map[string]string) (*Subscription, error) {
	if _, err := GetSubscription(name); err == nil {
		return nil, ErrAlreadyExistSubscription
	}
	topic, err := GetTopic(topicName)
	if err != nil {
		return nil, err
	}
	s := &Subscription{
		Name:               name,
		TopicID:            topic.Name,
		Message:            NewMessageStatusStore(name),
		DefaultAckDeadline: convertAckDeadlineSeconds(timeout),
		PushTick:           PushInterval,
		PushSize:           MinPushSize,
	}
	if err := s.SetPushConfig(endpoint, attr); err != nil {
		return nil, err
	}
	if err := s.Save(); err != nil {
		return nil, err
	}

	return s, nil
}

// GetSubscription return Subscription object
func GetSubscription(name string) (*Subscription, error) {
	return getGlobalSubscription().Get(name)
}

// Delete is delete subscription at globalSubscription
func (s *Subscription) Delete() error {
	return getGlobalSubscription().Delete(s.Name)
}

// ListSubscription returns subscription list from globalSubscription
func ListSubscription() ([]*Subscription, error) {
	return getGlobalSubscription().List()
}

// RegisterMessage associate Message to Subscription
func (s *Subscription) RegisterMessage(msg *Message) error {
	if _, err := s.Message.NewMessageStatus(s.Name, msg.ID, s.DefaultAckDeadline); err != nil {
		return err
	}
	if err := s.Save(); err != nil {
		return err
	}

	// push
	if !s.isPullMode() {
		_, err := s.Push(1)
		return err
	}

	return nil
}

// PullMessage represent Message and AckID pair
type PullMessage struct {
	AckID   string   `json:"ack_id"`
	Message *Message `json:"message"`
}

// Pull returns readable messages, and change message state
func (s *Subscription) Pull(size int) ([]*PullMessage, error) {
	msgs, err := s.Message.CollectReadableMessage(size)
	if err != nil {
		return nil, err
	}

	pullMsgs := make([]*PullMessage, 0, len(msgs))
	for _, m := range msgs {
		ackID := makeAckID()
		if err := s.Message.Deliver(m.ID, ackID); err != nil {
			return nil, err
		}
		pullMsgs = append(pullMsgs, &PullMessage{AckID: ackID, Message: m})
	}
	return pullMsgs, nil
}

// sentState is state of send push message
type sentState int

const (
	_ sentState = iota
	sentSucceed
	sentFailed
	notSent
)

func (s sentState) String() string {
	switch s {
	case sentSucceed:
		return "Succeed"
	case sentFailed:
		return "Failed"
	case notSent:
		return "Not send"
	default:
		return "Unknown"
	}
}

// Push send message to push endpoint, returns send flag and error
func (s *Subscription) Push(size int) (sentState, error) {
	msgs, err := s.Message.CollectReadableMessage(size)
	if err != nil {
		// empty message is non error
		if errors.Cause(err) == ErrEmptyMessage {
			return notSent, nil
		}
		return notSent, err
	}
	for _, msg := range msgs {
		ackID := makeAckID()
		s.Message.Deliver(msg.ID, ackID)
		err := s.PushConfig.sendMessage(msg, s.Name)
		if err != nil {
			return sentFailed, err
		}
		s.Ack(ackID)
	}

	return sentSucceed, nil
}

// Succeed Message delivery. remove sent Message.
func (s *Subscription) Ack(ids ...string) error {
	// collect MessageID list dependent to AckID
	for _, id := range ids {
		if err := s.Message.Ack(id); err != nil {
			return err
		}
	}
	return nil
}

// ModifyAckDeadline modify message ack deadline seconds
func (s *Subscription) ModifyAckDeadline(id string, timeout int64) error {
	ms, err := s.Message.FindByAckID(id)
	if err != nil {
		return err
	}
	ms.AckDeadline = convertAckDeadlineSeconds(timeout)
	return ms.Save()
}

// SetPushConfig setting push endpoint with attributes
func (s *Subscription) SetPushConfig(endpoint string, attribute map[string]string) error {
	p, err := NewPush(endpoint, attribute)
	if err != nil {
		return err
	}

	s.PushConfig = p
	if p.HasValidEndpoint() {
		// set push
		if err := s.PushLoop(); err != nil {
			return err
		}
	} else {
		// set pull
		if s.getRunning() {
			if err := s.setAbortPush(true); err != nil {
				return err
			}
		}
	}
	return s.Save()
}

func (s *Subscription) isPullMode() bool {
	return !s.PushConfig.HasValidEndpoint()
}

// PushLoop goroutine that keeps looking for pushable messages
func (s *Subscription) PushLoop() error {
	// already running loop, or pull mode
	if s.getRunning() || s.isPullMode() {
		return nil
	}

	if err := s.setRunning(true); err != nil {
		return err
	}
	go func() {
		for {
			// refresh Subscription
			s, err := GetSubscription(s.Name)
			if err != nil {
				log.Println(err.Error())
				break
			}

			// check abort
			if s.getAbortPush() {
				break
			}

			state, err := s.Push(s.getSize())
			// improve push size, it is determined like TCP slow start
			if err != nil {
				log.Println(err.Error())
			}
			switch state {
			case sentSucceed:
				s.incrementPushSize()
			case sentFailed:
				s.decrementPushSize()
			}

			time.Sleep(s.PushTick)
		}

		if err := s.teardownPushLoop(); err != nil {
			log.Println(err.Error())
		}
	}()
	return nil
}

func (s *Subscription) teardownPushLoop() error {
	// goroutine safe
	s, err := GetSubscription(s.Name)
	if err != nil {
		return err
	}

	if err := s.setRunning(false); err != nil {
		return err
	}
	if err := s.setAbortPush(false); err != nil {
		return err
	}
	return nil
}

// getAbortPush return AbortPush at mutex
func (s *Subscription) getAbortPush() bool {
	s.abortMu.RLock()
	defer s.abortMu.RUnlock()

	return s.AbortPush
}

// setAbortPush setting AbortPush at mutex
func (s *Subscription) setAbortPush(b bool) error {
	s.abortMu.Lock()
	defer s.abortMu.Unlock()

	s.AbortPush = b
	return s.Save()
}

// getRunning return pushRunning at mutex
func (s *Subscription) getRunning() bool {
	s.runningMu.RLock()
	defer s.runningMu.RUnlock()

	return s.PushRunning
}

// setRunning setting pushRunning at mutex
func (s *Subscription) setRunning(b bool) error {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()

	s.PushRunning = b
	return s.Save()
}

// getSize return pushSize at mutex
func (s *Subscription) getSize() int {
	s.sizeMu.RLock()
	defer s.sizeMu.RUnlock()

	return s.PushSize
}

// setSize setting pushSize at mutex
func (s *Subscription) setSize(i int) error {
	s.sizeMu.Lock()
	defer s.sizeMu.Unlock()

	// goroutine safe
	s, err := GetSubscription(s.Name)
	if err != nil {
		return err
	}
	s.PushSize = i
	return s.Save()
}

func (s *Subscription) incrementPushSize() error {
	newSize := s.getSize() * 2
	if newSize > MaxPushSize {
		newSize = MaxPushSize
	}
	return s.setSize(newSize)
}

func (s *Subscription) decrementPushSize() error {
	newSize := int(s.getSize() / 2)
	if newSize < MinPushSize {
		newSize = MinPushSize
	}
	return s.setSize(newSize)
}

// convertAckDeadlineSeconds convert timeout to seconds time.Duration
func convertAckDeadlineSeconds(timeout int64) time.Duration {
	if timeout < 0 {
		timeout = 0
	}
	return time.Duration(timeout) * time.Second
}

// Save is save to datastore
func (s *Subscription) Save() error {
	return getGlobalSubscription().Set(s)
}

// BySubscriptionName implements sort.Interface for []*Subscription based on the ID
type BySubscriptionName []*Subscription

func (a BySubscriptionName) Len() int           { return len(a) }
func (a BySubscriptionName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BySubscriptionName) Less(i, j int) bool { return a[i].Name < a[j].Name }
