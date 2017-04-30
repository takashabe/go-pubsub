package models

import (
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
)

// MessageStatus is holds params for Message
type MessageStatus struct {
	ID             string
	SubscriptionID string
	MessageID      string
	AckID          string
	AckDeadline    time.Duration
	AckState       messageState
	DeliveredAt    time.Time
}

func newMessageStatus(msgID, subID string, deadline time.Duration) *MessageStatus {
	return &MessageStatus{
		ID:             makeMessageStatusID(subID, msgID),
		SubscriptionID: subID,
		MessageID:      msgID,
		AckID:          "",
		AckDeadline:    deadline,
		AckState:       stateWait,
	}
}

func makeMessageStatusID(subID, msgID string) string {
	return fmt.Sprintf("%s-%s", subID, msgID)
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

// Save save MessageStatus to backend datastore
func (m *MessageStatus) Save() error {
	return globalMessageStatus.Set(m)
}

// MessageStatusStore is holds and adapter for MessageStatus
type MessageStatusStore struct {
	Store *DatastoreMessageStatus
}

func newMessageStatusStore(cfg *Config) (*MessageStatusStore, error) {
	d, err := NewDatastoreMessageStatus(cfg)
	if err != nil {
		return nil, err
	}
	return &MessageStatusStore{
		Store: d,
	}, nil
}

// SaveStatus MessageStatus save to backend store
func (s *MessageStatusStore) SaveStatus(ms *MessageStatus) error {
	return s.Store.Set(ms)
}

// FindByMessageID return MessageStatus matched MessageID
func (s *MessageStatusStore) FindByMessageID(id string) (*MessageStatus, error) {
	return s.Store.FindByMessageID(id)
}

// FindByAckID return MessageStatus matched AckID
func (s *MessageStatusStore) FindByAckID(id string) (*MessageStatus, error) {
	return s.Store.FindByAckID(id)
}

// GetRangeMessage return readable messages
func (s *MessageStatusStore) GetRangeMessage(size int) ([]*Message, error) {
	storeLength := s.Store.Size()
	if storeLength == 0 {
		return nil, ErrEmptyMessage
	}
	if storeLength < size {
		size = storeLength
	}

	msgs, err := s.Store.CollectByReadableMessage(size)
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
	ms, err := s.Store.FindByMessageID(msgID)
	if err != nil {
		return ErrNotFoundEntry
	}
	if ms.AckState == stateAck {
		return ErrAlreadyReadMessage
	}
	ms.AckState = stateDeliver
	ms.AckID = ackID
	ms.DeliveredAt = time.Now()
	return s.SaveStatus(ms)
}

// Ack change state to ack for message
func (s *MessageStatusStore) Ack(ackID string) error {
	ms, err := s.Store.FindByAckID(ackID)
	if err != nil {
		return ErrNotFoundEntry
	}
	m, err := globalMessage.Get(ms.MessageID)
	if err != nil {
		return ErrNotFoundEntry
	}
	m.AckSubscription(ms.SubscriptionID)
	if err := m.Save(); err != nil {
		return err
	}
	s.Store.Delete(m.ID)
	if len(m.Subscriptions.Dump()) == 0 {
		if err := m.Delete(); err != nil {
			return err
		}
	}
	return nil
}
