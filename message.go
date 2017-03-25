package queue

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

// Message errors
var (
	ErrEmptyMessage = errors.New("empty message")
)

type Message struct {
	ID          string
	Data        []byte
	Attributes  *Attributes
	Topic       *Topic
	acks        *acks
	PublishedAt time.Time
}

// acks repsents Subscriptions and Ack map.
type acks struct {
	list map[string]bool
	mu   sync.Mutex
}

func (s *acks) ack(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.list[id]; !ok {
		// NOP
		return
	}
	s.list[id] = true
}

func (s *acks) add(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.list[id] = false
}

func makeMessageID() string {
	return uuid.NewV1().String()
}

// MessageList is Message slice as queue.
type MessageList struct {
	list []*Message
	mu   sync.Mutex
}

// Append message to list
func (m *MessageList) Append(message *Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list = append(m.list, message)
}

// Get some messages
func (m *MessageList) GetRange(size int) ([]*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var ret []*Message
	maxLen := len(m.list)
	if maxLen == 0 {
		return nil, ErrEmptyMessage
	}

	// non error, when request over size
	if maxLen < size {
		ret = m.list[:]
		m.list = m.list[maxLen:]
		return ret, nil
	}

	ret = m.list[:size]
	m.list = m.list[size:]
	return ret, nil
}

func (m *MessageList) Ack(subID, messID string) {
	// TODO: too slow
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, v := range m.list {
		if v.ID == messID {
			v.acks.ack(subID)
			return
		}
	}
}
