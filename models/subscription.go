package models

import (
	"sort"
	"time"

	"github.com/pkg/errors"
)

type Subscription struct {
	Name       string        `json:"name"`
	Topic      *Topic        `json:"-"`
	Messages   *MessageList  `json:"-"`
	AckTimeout time.Duration `json:"ack_deadline_seconds"`
	Push       *Push         `json:"push_config"`
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
		Name:     name,
		Topic:    topic,
		Messages: newMessageList(),
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

// Deliver Message
func (s *Subscription) Pull(size int) ([]*Message, error) {
	messages, err := s.Messages.GetRange(s, size)
	if err != nil {
		return nil, err
	}
	for _, m := range messages {
		m.Deliver(s.Name)
	}

	return messages, nil
}

// Succeed Message delivery. remove sent Message.
func (s *Subscription) Ack(ids ...string) {
	for _, id := range ids {
		s.Messages.Ack(s.Name, id)
	}
}

// Set Ack timeout, arg time expect second.
func (s *Subscription) SetAckTimeout(timeout int64) {
	if timeout < 0 {
		timeout = 0
	}
	s.AckTimeout = time.Duration(timeout) * time.Second
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

// MessageList Message slice behavior like queue
type MessageList struct {
	list *DatastoreMessage
}

func newMessageList() *MessageList {
	return &MessageList{
		list: globalMessage,
	}
}

// GetRange returns readable message
func (m *MessageList) GetRange(sub *Subscription, size int) ([]*Message, error) {
	maxLen := m.list.Size()
	if maxLen == 0 {
		return nil, ErrEmptyMessage
	}
	// non error, when request over size
	if maxLen < size {
		size = maxLen
	}

	msgs, err := m.list.FindByReadable(sub.Name, sub.AckTimeout, size)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get message dependent Subscription name=%s", sub.Name)
	}
	if len(msgs) == 0 {
		return nil, ErrEmptyMessage
	}

	sort.Sort(ByMessageID(msgs))
	return msgs, nil
}

// Ack is change message state and remove message
func (m *MessageList) Ack(subID, messID string) error {
	msg, err := m.list.Get(messID)
	if err != nil {
		return errors.Wrapf(err, "failed to ack message key=%s, subscription=%s", messID, subID)
	}
	msg.Ack(subID)
	if err := msg.Save(); err != nil {
		return err
	}
	if err := msg.Delete(); err != nil {
		// ignore error
		if err == errors.Cause(ErrNotYetReceivedAck) {
			return nil
		}
		return err
	}
	return nil
}

// BySubscriptionName implements sort.Interface for []*Subscription based on the ID
type BySubscriptionName []*Subscription

func (a BySubscriptionName) Len() int           { return len(a) }
func (a BySubscriptionName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BySubscriptionName) Less(i, j int) bool { return a[i].Name < a[j].Name }
