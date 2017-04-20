package models

import (
	"time"

	"github.com/satori/go.uuid"
)

type messageState int

const (
	_ messageState = iota
	stateWait
	stateDeliver
	stateAck
)

func (s messageState) String() string {
	switch s {
	case stateWait:
		return "Waiting"
	case stateDeliver:
		return "Delivered"
	case stateAck:
		return "Ack"
	default:
		return "Non define"
	}
}

// Message is data object
type Message struct {
	ID            string            `json:"message_id"`
	Data          []byte            `json:"data"`
	Attributes    map[string]string `json:"attributes"`
	Subscriptions *Memory           `json:"-"`
	PublishedAt   time.Time         `json:"publish_time"`
	DeliveredAt   time.Time         `json:"-"`
}

func makeMessageID() string {
	return uuid.NewV1().String()
}

func makeAckID() string {
	return uuid.NewV1().String()
}

func NewMessage(id string, data []byte, attr map[string]string, subs []*Subscription) *Message {
	m := &Message{
		ID:            id,
		Data:          data,
		Attributes:    attr,
		Subscriptions: NewMemory(nil),
		PublishedAt:   time.Now(),
	}
	for _, sub := range subs {
		m.AddSubscription(sub.Name)
	}
	return m
}

func (m *Message) AddSubscription(name string) {
	m.Subscriptions.Set(name, struct{}{})
}

func (m *Message) AckSubscription(subID string) error {
	return m.Subscriptions.Delete(subID)
}

// Save is save message to datastore
func (m *Message) Save() error {
	return globalMessage.Set(m)
}

// Delete is received all ack response message to delete
func (m *Message) Delete() error {
	return globalMessage.Delete(m.ID)
}

// ByMessageID implements sort.Interface for []*Message based on the ID
type ByMessageID []*Message

func (a ByMessageID) Len() int           { return len(a) }
func (a ByMessageID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByMessageID) Less(i, j int) bool { return a[i].ID < a[j].ID }
