package queue

import (
	"errors"
	"sync"
	"time"

	"github.com/k0kubun/pp"
)

// Subscription errors
var (
	ErrEmptyMessage = errors.New("empty message")
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
	if err := topic.AddSubscription(*s); err != nil {
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
		pp.Println("in Subscription", m)
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
	mu   sync.Mutex
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
	m.mu.Lock()
	defer m.mu.Unlock()

	maxLen := len(m.list)
	if maxLen == 0 {
		return nil, ErrEmptyMessage
	}

	// non error, when request over size
	if maxLen < size {
		size = maxLen
	}

	// pick messages and reappend nonReadable messages
	readable, nonReadable := m.collectMessage(sub, size)
	m.list = m.list[len(readable)+len(nonReadable):]
	for _, reAdd := range nonReadable {
		m.list = append(m.list, reAdd)
	}
	return readable, nil
}

// collect readable and non readable messages
func (m *MessageList) collectMessage(sub *Subscription, size int) (readable []*Message, nonReadable []*Message) {
	readable = make([]*Message, 0)
	nonReadable = make([]*Message, 0)

	for _, v := range m.list {
		if len(readable) > size {
			return readable, nonReadable
		}
		if v.Readable(sub.name, sub.ackTimeout) {
			readable = append(readable, v)
		} else {
			nonReadable = append(nonReadable, v)
		}
	}
	return readable, nonReadable
}

func (m *MessageList) Ack(subID, messID string) {
	// TODO: too slow
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, v := range m.list {
		if v.ID == messID {
			v.Ack(subID)
			return
		}
	}
}
