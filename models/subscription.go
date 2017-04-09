package models

import (
	"sort"
	"time"

	"github.com/pkg/errors"
)

type Subscription struct {
	name       string
	topic      *Topic
	messages   *MessageList
	ackTimeout time.Duration
	push       *Push
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
		name:     name,
		topic:    topic,
		messages: newMessageList(),
	}
	s.SetAckTimeout(timeout)
	if err := s.SetPush(endpoint, attr); err != nil {
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
	return globalSubscription.Delete(s.name)
}

// ListSubscription returns subscription list from globalSubscription
func ListSubscription() ([]*Subscription, error) {
	return globalSubscription.List()
}

// Deliver Message
func (s *Subscription) Pull(size int) ([]*Message, error) {
	messages, err := s.messages.GetRange(s, size)
	if err != nil {
		return nil, err
	}
	for _, m := range messages {
		m.Deliver(s.name)
	}

	return messages, nil
}

// Succeed Message delivery. remove sent Message.
func (s *Subscription) Ack(ids ...string) {
	for _, id := range ids {
		s.messages.Ack(s.name, id)
	}
}

// Set Ack timeout, arg time expect millisecond.
func (s *Subscription) SetAckTimeout(timeout int64) {
	if timeout < 0 {
		timeout = 0
	}
	s.ackTimeout = time.Duration(timeout) * time.Millisecond
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
	s.push = p
	return nil
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

	msgs, err := m.list.FindByReadable(sub.name, sub.ackTimeout, size)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get message dependent Subscription name=%s", sub.name)
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
	if err := m.list.Delete(messID); err != nil {
		return err
	}
	return nil
}
