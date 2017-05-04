package models

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/k0kubun/pp"
	"github.com/pkg/errors"
)

// messageState is represent Message deliver, ack status
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

// Readable return whether the message can be read
func (ms *MessageStatus) Readable() bool {
	switch ms.AckState {
	case stateAck:
		return false
	case stateDeliver:
		lapsedTime := time.Now().Sub(ms.DeliveredAt)
		pp.Println("lapsedTime = ", lapsedTime, ", deadline = ", ms.AckDeadline)
		return lapsedTime > ms.AckDeadline
	case stateWait:
		return true
	default:
		return false
	}
}

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

func newMessageStatus(subID, msgID string, deadline time.Duration) *MessageStatus {
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

// Deliver setting deliver state and new AckID
func (ms *MessageStatus) Deliver(ackID string) {
	ms.AckState = stateDeliver
	ms.AckID = ackID
	ms.DeliveredAt = time.Now()
}

// Save save MessageStatus to backend datastore
func (ms *MessageStatus) Save() error {
	return globalMessageStatus.Set(ms)
}

// Delete delete MessageStatus from backend datastore
func (ms *MessageStatus) Delete() error {
	return globalMessageStatus.Delete(ms.ID)
}

// MessageStatusStore is holds and adapter for MessageStatus
type MessageStatusStore struct {
	Status []string
}

// NewMessageStatusStore return created MessageStatusStore
func NewMessageStatusStore() *MessageStatusStore {
	return &MessageStatusStore{
		Status: make([]string, 0),
	}
}

// NewMessageStatus return created MessageStatus and save datastore
func (mss *MessageStatusStore) NewMessageStatus(subID, msgID string, deadline time.Duration) (*MessageStatus, error) {
	ms := newMessageStatus(subID, msgID, deadline)
	if err := ms.Save(); err != nil {
		return nil, err
	}
	mss.Status = append(mss.Status, ms.ID)
	return ms, nil
}

// CollectReadableMessage return readable messages
func (mss *MessageStatusStore) CollectReadableMessage(size int) ([]*Message, error) {
	// check size
	storeLength := len(mss.Status)
	if storeLength == 0 {
		return nil, ErrEmptyMessage
	}
	if storeLength < size {
		size = storeLength
	}

	// collect messages
	msList, err := globalMessageStatus.CollectByIDs(mss.Status...)
	if err != nil {
		return nil, err
	}
	res := make([]*Message, 0)
	for _, ms := range msList {
		if len(res) >= size {
			break
		}
		if ms.Readable() {
			m, err := globalMessage.Get(ms.MessageID)
			if err != nil {
				log.Println("failed to get message, id=%s, error=%v", ms.MessageID, err)
				continue
			}
			res = append(res, m)
		}
	}

	// return messages
	if len(res) == 0 {
		return nil, ErrEmptyMessage
	}
	sort.Sort(ByMessageID(res))
	return res, nil
}

// Deliver register AckID to message
func (mss *MessageStatusStore) Deliver(msgID, ackID string) error {
	ms, err := globalMessageStatus.FindByMessageID(msgID)
	if err != nil {
		return err
	}
	if ms.AckState == stateAck {
		return ErrAlreadyReadMessage
	}
	ms.Deliver(ackID)
	return ms.Save()
}

// Ack invisible message depends ackID
func (mss *MessageStatusStore) Ack(ackID string) error {
	ms, err := globalMessageStatus.FindByAckID(ackID)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to FindByAckID, AckID=%s", ackID))
	}
	m, err := globalMessage.Get(ms.MessageID)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to get message, MessageID=%s", ms.MessageID))
	}
	if err := m.AckSubscription(ms.SubscriptionID); err != nil {
		return errors.Wrap(err, "failed to ack subscription")
	}
	if err := m.Save(); err != nil {
		return err
	}

	// delete Message and MessageStatus
	if err := ms.Delete(); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to delete message status, MessageStatusID=%s", ms.ID))
	}
	if len(m.Subscriptions.Dump()) == 0 {
		if err := m.Delete(); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to delete message, MessageID=%s", m.ID))
		}
	}
	return nil
}

// FindByAckID return MessageStatus depends AckID
func (mss *MessageStatusStore) FindByAckID(ackID string) (*MessageStatus, error) {
	return globalMessageStatus.FindByAckID(ackID)
}
