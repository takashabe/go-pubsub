package models

import (
	"sort"
	"time"

	"github.com/pkg/errors"
)

type Subscription struct {
	Name               string              `json:"name"`
	Topic              *Topic              `json:"-"`
	DefaultAckDeadline time.Duration       `json:"ack_deadline_seconds"`
	MessageStatus      *MessageStatusStore `json:"-"`
	Push               *Push               `json:"push_config"`
}

// Create Subscription, if not exist already same name Subscription
func NewSubscription(name, topicName string, timeout int64, endpoint string, attr map[string]string) (*Subscription, error) {
	if _, err := GetSubscription(name); err == nil {
		return nil, ErrAlreadyExistSubscription
	}

	topic, err := GetTopic(topicName)
	if err != nil {
		return nil, err
	}
	ms, err := newMessageStatusStore(globalConfig)
	if err != nil {
		return nil, err
	}
	s := &Subscription{
		Name:          name,
		Topic:         topic,
		MessageStatus: ms,
	}
	s.SetAckTimeout(timeout)
	if err := s.SetPush(endpoint, attr); err != nil {
		return nil, err
	}
	if err := s.Save(); err != nil {
		return nil, err
	}
	if err := topic.AddSubscription(s); err != nil {
		return nil, err
	}

	return s, nil
}

// GetSubscription return Subscription object
func GetSubscription(name string) (*Subscription, error) {
	return globalSubscription.Get(name)
}

// Delete is delete subscription at globalSubscription
func (s *Subscription) Delete() error {
	if err := s.Topic.DeleteSubscription(s.Name); err != nil {
		return err
	}
	return globalSubscription.Delete(s.Name)
}

// ListSubscription returns subscription list from globalSubscription
func ListSubscription() ([]*Subscription, error) {
	return globalSubscription.List()
}

// RegisterMessage associate Message to Subscription
func (s *Subscription) RegisterMessage(msg *Message) error {
	s.MessageStatus.Set(newMessageStatus(msg.ID, s.Name, s.DefaultAckDeadline))
	return s.Save()
}

// PullMessage represent Message and AckID pair
type PullMessage struct {
	AckID   string   `json:"ack_id"`
	Message *Message `json:"message"`
}

// Pull returns readable messages, and change message state
func (s *Subscription) Pull(size int) ([]*PullMessage, error) {
	msgs, err := s.MessageStatus.GetRangeMessage(size)
	if err != nil {
		return nil, err
	}

	pullMsgs := make([]*PullMessage, 0, len(msgs))
	for _, m := range msgs {
		ackID := makeAckID()
		if err := s.MessageStatus.Deliver(m.ID, ackID); err != nil {
			return nil, err
		}
		pullMsgs = append(pullMsgs, &PullMessage{AckID: ackID, Message: m})
	}
	return pullMsgs, nil
}

// Succeed Message delivery. remove sent Message.
func (s *Subscription) Ack(ids ...string) error {
	// collect MessageID list dependent to AckID
	msgIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		ms, err := s.MessageStatus.FindByAckID(id)
		if err != nil {
			return err
		}
		msgIDs = append(msgIDs, ms.MessageID)
	}
	// ack for message
	for _, id := range msgIDs {
		s.MessageStatus.Ack(id)
	}
	return nil
}

// Set Ack timeout, arg time expect second.
func (s *Subscription) SetAckTimeout(timeout int64) {
	if timeout < 0 {
		timeout = 0
	}
	s.DefaultAckDeadline = time.Duration(timeout) * time.Second
}

// Set push endpoint with attributes, only one can be set as push endpoint.
func (s *Subscription) SetPush(endpoint string, attribute map[string]string) error {
	if len(endpoint) == 0 {
		return nil
	}

	p, err := NewPush(endpoint, attribute)
	if err != nil {
		return err
	}
	s.Push = p
	return nil
}

// Save is save to datastore
func (s *Subscription) Save() error {
	return globalSubscription.Set(s)
}

// MessageStatusStore is holds and adapter for MessageStatus
type MessageStatusStore struct {
	store *DatastoreMessageStatus
}

func newMessageStatusStore(cfg *Config) (*MessageStatusStore, error) {
	d, err := NewDatastoreMessageStatus(cfg)
	if err != nil {
		return nil, err
	}
	return &MessageStatusStore{
		store: d,
	}, nil
}

// Set MessageStatus save to backend store
func (s *MessageStatusStore) Set(ms *MessageStatus) error {
	return s.store.Set(ms)
}

// FindByMessageID return MessageStatus matched MessageID
func (s *MessageStatusStore) FindByMessageID(id string) (*MessageStatus, error) {
	return s.store.FindByMessageID(id)
}

// FindByAckID return MessageStatus matched AckID
func (s *MessageStatusStore) FindByAckID(id string) (*MessageStatus, error) {
	return s.store.FindByAckID(id)
}

// GetRangeMessage return readable messages
func (s *MessageStatusStore) GetRangeMessage(size int) ([]*Message, error) {
	storeLength := s.store.Size()
	if storeLength == 0 {
		return nil, ErrEmptyMessage
	}
	if storeLength < size {
		size = storeLength
	}

	msgs, err := s.store.CollectByReadableMessage(size)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get range messages")
	}
	if len(msgs) == 0 {
		return nil, ErrEmptyMessage
	}
	sort.Sort(ByMessageID(msgs))
	return msgs, nil
}

func (s *MessageStatusStore) Deliver(msgID, ackID string) error {
	ms, err := s.store.FindByMessageID(msgID)
	if err != nil {
		return ErrNotFoundMessageStatus
	}
	if ms.AckState == stateAck {
		return ErrAlreadyReadMessage
	}
	ms.AckState = stateDeliver
	ms.AckID = ackID
	ms.DeliveredAt = time.Now()
	return s.Set(ms)
}

// Ack change state to ack for message
func (s *MessageStatusStore) Ack(id string) error {
	ms, err := s.store.FindByAckID(id)
	if err != nil {
		return ErrNotFoundMessageStatus
	}
	m, err := globalMessage.Get(ms.MessageID)
	if err != nil {
		return ErrNotFoundMessage
	}
	m.AckSubscription(ms.SubscriptionID)
	if err := m.Save(); err != nil {
		return err
	}
	return nil
}

// MessageStatus is holds params for Message
type MessageStatus struct {
	MessageID      string
	SubscriptionID string
	AckID          string
	AckDeadline    time.Duration
	AckState       messageState
	DeliveredAt    time.Time
}

func newMessageStatus(msgID, subID string, deadline time.Duration) *MessageStatus {
	return &MessageStatus{
		MessageID:      msgID,
		SubscriptionID: subID,
		AckID:          "",
		AckDeadline:    deadline,
		AckState:       stateWait,
	}
}

// Readable return whether the message can be read
func (m *MessageStatus) Readable() bool {
	switch m.AckState {
	case stateAck:
		return false
	case stateDeliver:
		return time.Now().Sub(m.DeliveredAt) > m.AckDeadline
	case stateWait:
		return true
	default:
		return false
	}
}

// BySubscriptionName implements sort.Interface for []*Subscription based on the ID
type BySubscriptionName []*Subscription

func (a BySubscriptionName) Len() int           { return len(a) }
func (a BySubscriptionName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BySubscriptionName) Less(i, j int) bool { return a[i].Name < a[j].Name }
