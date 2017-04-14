package models

import (
	"fmt"

	"github.com/pkg/errors"
)

// Topic object
type Topic struct {
	Name string                 `json:"name"`
	Sub  *DatastoreSubscription `json:"-"`
}

// Create topic, if not exist already topic name in GlobalTopics
func NewTopic(name string) (*Topic, error) {
	if _, err := GetTopic(name); err == nil {
		return nil, ErrAlreadyExistTopic
	}

	// TODO: fix me. dont require specific datastore
	d, err := NewDatastoreSubscription(&Config{Driver: "memory"})
	if err != nil {
		return nil, err
	}
	t := &Topic{
		Name: name,
		Sub:  d,
	}
	if err := t.Save(); err != nil {
		return nil, errors.Wrapf(err, "failed to save topic, name=%s", name)
	}
	return t, nil
}

// Return topic object
func GetTopic(name string) (*Topic, error) {
	t, err := globalTopics.Get(name)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// Return topic list
func ListTopic() ([]*Topic, error) {
	return globalTopics.List()
}

// Delete topic object at GlobalTopics
func (t *Topic) Delete() error {
	return globalTopics.Delete(t.Name)
}

// Register subscription
func (t *Topic) AddSubscription(s *Subscription) error {
	if _, err := t.Sub.Get(s.Name); err == nil {
		return errors.Wrapf(ErrAlreadyExistSubscription, fmt.Sprintf("id=%s", s.Name))
	}
	return t.Sub.Set(s)
}

// Publish is Message save and deliver to subscription, and return message id
func (t *Topic) Publish(data []byte, attributes map[string]string) (string, error) {
	subList, err := t.GetSubscriptions()
	if err != nil {
		return "", errors.Wrap(err, "failed GetSubscriptions")
	}
	m := NewMessage(makeMessageID(), *t, data, attributes, subList)
	if err := m.Save(); err != nil {
		return "", errors.Wrap(err, "failed save Message")
	}
	return m.ID, nil
}

// GetSubscription return a topic dependent Subscription
func (t *Topic) GetSubscription(key string) (*Subscription, error) {
	return t.Sub.Get(key)
}

// GetSubscriptions returns topic dependent Subscription list
func (t *Topic) GetSubscriptions() ([]*Subscription, error) {
	return t.Sub.List()
}

// DeleteSubscription is delete depends subscription
func (t *Topic) DeleteSubscription(name string) error {
	if err := t.Sub.Delete(name); err != nil {
		return err
	}
	return t.Save()
}

// Save is save to datastore
func (t *Topic) Save() error {
	return globalTopics.Set(t)
}

// ByTopicName implements sort.Interface for []*Topic based on the ID
type ByTopicName []*Topic

func (a ByTopicName) Len() int           { return len(a) }
func (a ByTopicName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTopicName) Less(i, j int) bool { return a[i].Name < a[j].Name }
