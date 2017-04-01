package queue

import (
	"fmt"

	"github.com/pkg/errors"
)

// Topic errors
var (
	ErrAlreadyExistTopic        = errors.New("already exist topic")
	ErrAlreadyExistSubscription = errors.New("already exist subscription")
	ErrNotFoundTopic            = errors.New("not found topic")
	ErrNotHasSubscription       = errors.New("not has subscription")
	ErrInvalidTopic             = errors.New("invalid topic")
)

// Global variable Topic map
var GlobalTopics *DatastoreTopic = new(DatastoreTopic)

// Topic object
type Topic struct {
	name string
	sub  *DatastoreSubscription
}

// Create topic, if not exist already topic name in GlobalTopics
func NewTopic(name string) (*Topic, error) {
	if _, ok := GlobalTopics.Get(name); ok {
		return nil, ErrAlreadyExistTopic
	}

	t := &Topic{
		name: name,
		sub:  NewDatastoreSubscription(),
	}
	GlobalTopics.Set(t)
	return t, nil
}

// Return topic object
func GetTopic(name string) (*Topic, error) {
	t, ok := GlobalTopics.Get(name)
	if !ok {
		return nil, ErrNotFoundTopic
	}

	return t, nil
}

// Return topic list
func ListTopic() ([]*Topic, error) {
	return GlobalTopics.List()
}

// Delete topic object at GlobalTopics
func (t *Topic) Delete() {
	GlobalTopics.Delete(t.name)
}

// Register subscription
func (t *Topic) AddSubscription(s *Subscription) error {
	if _, err := t.sub.Get(s.name); err == nil {
		return errors.Wrapf(ErrAlreadyExistSubscription, fmt.Sprintf("id=%s", s.name))
	}
	return t.sub.Set(s)
}

// Message store backend storage and delivery to Subscription
func (t *Topic) Publish(data []byte, attributes map[string]string) error {
	subList, err := t.GetSubscriptions()
	if err != nil {
		return errors.Wrap(err, "failed GetSubscriptions")
	}
	m := NewMessage(makeMessageID(), *t, data, attributes, subList)
	// TODO: save message to store

	for _, s := range subList {
		s.Subscribe(m)
	}
	return nil
}

// GetSubscription return a topic dependent Subscription
func (t *Topic) GetSubscription(key string) (*Subscription, error) {
	return t.sub.Get(key)
}

// GetSubscriptions returns topic dependent Subscription list
func (t *Topic) GetSubscriptions() ([]*Subscription, error) {
	return t.sub.List()
}
