package models

import (
	"time"

	"github.com/satori/go.uuid"
)

// Message is data object
type Message struct {
	ID           string            `json:"message_id"`
	Data         []byte            `json:"data"`
	Attributes   map[string]string `json:"attributes"`
	SubscribeIDs []string          `json:"-"`
	PublishedAt  time.Time         `json:"publish_time"`
}

func makeMessageID() string {
	return uuid.NewV1().String()
}

func makeAckID() string {
	return uuid.NewV1().String()
}

func NewMessage(id string, data []byte, attr map[string]string, subs []*Subscription) *Message {
	m := &Message{
		ID:           id,
		Data:         data,
		Attributes:   attr,
		SubscribeIDs: make([]string, 0),
		PublishedAt:  time.Now(),
	}
	for _, sub := range subs {
		m.AddSubscription(sub.Name)
	}
	return m
}

func (m *Message) AddSubscription(subID string) error {
	m.SubscribeIDs = append(m.SubscribeIDs, subID)
	return m.Save()
}

// AckSubscription remove Subscription
func (m *Message) AckSubscription(subID string) error {
	for k, v := range m.SubscribeIDs {
		if subID == v {
			m.SubscribeIDs = append(m.SubscribeIDs[:k], m.SubscribeIDs[k+1:]...)
			return m.Save()
		}
	}
	return nil
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
