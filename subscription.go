package queue

import (
	"errors"
	"sync"
	"time"
)

// Subscription errors
var (
	ErrEmptyMessage             = errors.New("empty message")
	ErrNotFoundSubscription     = errors.New("not found subscription")
	ErrNotMatchTypeSubscription = errors.New("not match type subscription")
)

type Subscription struct {
	name       string
	topic      *Topic
	messages   *MessageList
	ackTimeout time.Duration
	push       *Push
}

// Create Subscription
func NewSubscription(name, topicName string, timeout int64, endpoint string, attr map[string]string) (*Subscription, error) {
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

// Receive Message from Topic
func (s *Subscription) Subscribe(m *Message) {
	s.messages.Append(m)
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

// MessageList is message slice as queue
type MessageList struct {
	list []*Message
	mu   sync.RWMutex
}

func newMessageList() *MessageList {
	return &MessageList{
		list: make([]*Message, 0),
	}
}

// Append message to list
func (m *MessageList) Append(message *Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list = append(m.list, message)
}

// GetRange returns readable message
func (m *MessageList) GetRange(sub *Subscription, size int) ([]*Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	maxLen := len(m.list)
	if maxLen == 0 {
		return nil, ErrEmptyMessage
	}

	// non error, when request over size
	if maxLen < size {
		size = maxLen
	}

	readable := m.collectMessage(sub, size)
	if len(readable) == 0 {
		return nil, ErrEmptyMessage
	}
	return readable, nil
}

// collect readable and non readable messages
func (m *MessageList) collectMessage(sub *Subscription, size int) []*Message {
	msgs := make([]*Message, 0)

	for _, v := range m.list {
		if len(msgs) > size {
			return msgs
		}
		if v.Readable(sub.name, sub.ackTimeout) {
			msgs = append(msgs, v)
		}
	}
	return msgs
}

// Ack is change message state and remove message
func (m *MessageList) Ack(subID, messID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, v := range m.list {
		if v.ID == messID {
			v.Ack(subID)
			// remove ack message
			m.list = append(m.list[:i], m.list[(i+1):]...)
			return
		}
	}
}
