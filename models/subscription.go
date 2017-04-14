package models

import (
	"sort"
	"time"

	"github.com/pkg/errors"
)

type Subscription struct {
	Name               string           `json:"name"`
	Topic              *Topic           `json:"-"`
	Messages           *MessageList     `json:"-"`
	AckMessages        *AckMessages     `json:"-"`
	DefaultAckDeadline time.Duration    `json:"ack_deadline_seconds"`
	MessageStatus      []*MessageStatus `json:"-"`
	Push               *Push            `json:"push_config"`
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
		Name:          name,
		Topic:         topic,
		Messages:      newMessageList(),
		AckMessages:   newAckMessages(),
		MessageStatus: make([]*MessageStatus, 0),
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

// RegisterMessage associate Message to Subscription
func (s *Subscription) RegisterMessage(msg *Message) error {
	ms := newMessageStatus(msg.ID, s.DefaultAckDeadline)
	s.MessageStatus = append(s.MessageStatus, ms)
	return s.Save()
}

// PullMessage represent Message and AckID pair
type PullMessage struct {
	AckID   string   `json:"ack_id"`
	Message *Message `json:"message"`
}

// Deliver Message
func (s *Subscription) Pull(size int) ([]*PullMessage, error) {
	messages, err := s.Messages.GetRange(s, size)
	if err != nil {
		return nil, err
	}

	pullMsgs := make([]*PullMessage, 0, len(messages))
	for _, m := range messages {
		m.Deliver(s.Name)
		ackID := makeAckID()
		if err := s.AckMessages.setAckID(ackID, m.ID); err != nil {
			return nil, err
		}
		pullMsgs = append(pullMsgs, &PullMessage{AckID: ackID, Message: m})
	}
	return pullMsgs, nil
}

// Succeed Message delivery. remove sent Message.
func (s *Subscription) Ack(ids ...string) error {
	// collect MessageID list dependent to AckID
	msgIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		msgID, ok := s.AckMessages.getMessageID(id)
		if !ok {
			return ErrNotFoundAckID
		}
		msgIDs = append(msgIDs, msgID)
	}
	// ack for message
	for _, id := range msgIDs {
		s.Messages.Ack(s.Name, id)
		s.AckMessages.delete(id)
	}
	return nil
}

// Set Ack timeout, arg time expect second.
func (s *Subscription) SetAckTimeout(timeout int64) {
	if timeout < 0 {
		timeout = 0
	}
	s.DefaultAckDeadline = time.Duration(timeout) * time.Second
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

	msgs, err := m.list.FindByReadable(sub.Name, sub.DefaultAckDeadline, size)
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

// AckMessages is holds MessageID and AckID pairs
type AckMessages struct {
	list Datastore
}

func newAckMessages() *AckMessages {
	return &AckMessages{
		list: NewMemory(nil),
	}
}

func (a *AckMessages) getMessageID(ackID string) (string, bool) {
	for k, v := range a.list.Dump() {
		if v == ackID {
			return k.(string), true
		}
	}
	return "", false
}

func (a *AckMessages) setAckID(ackID, msgID string) error {
	return a.list.Set(msgID, ackID)
}

func (a *AckMessages) delete(id string) error {
	return a.list.Delete(id)
}

// MessageStatus is holds params for Message
type MessageStatus struct {
	MessageID   string
	AckID       string
	AckDeadline time.Duration
	AckState    messageState
}

func newMessageStatus(msgID string, deadline time.Duration) *MessageStatus {
	return &MessageStatus{
		MessageID:   msgID,
		AckID:       "",
		AckDeadline: deadline,
		AckState:    stateWait,
	}
}

// BySubscriptionName implements sort.Interface for []*Subscription based on the ID
type BySubscriptionName []*Subscription

func (a BySubscriptionName) Len() int           { return len(a) }
func (a BySubscriptionName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BySubscriptionName) Less(i, j int) bool { return a[i].Name < a[j].Name }
