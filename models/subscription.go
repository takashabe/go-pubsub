package models

import "time"

type Subscription struct {
	Name               string              `json:"name"`
	TopicID            string              `json:"topic"`
	Message            *MessageStatusStore `json:"-"`
	DefaultAckDeadline time.Duration       `json:"ack_deadline_seconds"`
	PushConfig         *Push               `json:"push_config"`
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
	s := &Subscription{
		Name:               name,
		TopicID:            topic.Name,
		Message:            NewMessageStatusStore(name),
		DefaultAckDeadline: convertAckDeadlineSeconds(timeout),
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
	return globalSubscription.Get(name)
}

// Delete is delete subscription at globalSubscription
func (s *Subscription) Delete() error {
	return globalSubscription.Delete(s.Name)
}

// ListSubscription returns subscription list from globalSubscription
func ListSubscription() ([]*Subscription, error) {
	return globalSubscription.List()
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
	if s.PushConfig.HasValidEndpoint() {
		// TODO: implements push loop goroutine
		return s.Push()
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

// Push send message to push endpoint
func (s *Subscription) Push() error {
	// TODO: implements slow start size. it means, size increment when success push
	msgs, err := s.Message.CollectReadableMessage(3)
	if err != nil {
		return err
	}
	for _, msg := range msgs {
		ackID := makeAckID()
		s.Message.Deliver(msg.ID, ackID)
		err := s.PushConfig.sendMessage(msg, s.Name)
		if err != nil {
			return err
		}
		s.Ack(ackID)
	}

	return nil
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
	return s.Save()
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
	return globalSubscription.Set(s)
}

// BySubscriptionName implements sort.Interface for []*Subscription based on the ID
type BySubscriptionName []*Subscription

func (a BySubscriptionName) Len() int           { return len(a) }
func (a BySubscriptionName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BySubscriptionName) Less(i, j int) bool { return a[i].Name < a[j].Name }
