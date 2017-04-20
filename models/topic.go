package models

import "github.com/pkg/errors"

// Topic object
type Topic struct {
	Name string `json:"name"`
}

// Create topic, if not exist already topic name in GlobalTopics
func NewTopic(name string) (*Topic, error) {
	if _, err := GetTopic(name); err == nil {
		return nil, ErrAlreadyExistTopic
	}
	t := &Topic{
		Name: name,
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

// Publish create message and deliver to subscription, and return created message id
func (t *Topic) Publish(data []byte, attr map[string]string) (string, error) {
	subList, err := t.GetSubscriptions()
	if err != nil {
		return "", errors.Wrap(err, "failed GetSubscriptions")
	}

	// TODO: need transaction
	m := NewMessage(makeMessageID(), data, attr, subList)
	if err := m.Save(); err != nil {
		return "", errors.Wrap(err, "failed save Message")
	}
	for _, s := range subList {
		if err := s.RegisterMessage(m); err != nil {
			return "", err
		}
	}
	return m.ID, nil
}

// GetSubscriptions returns topic dependent Subscription list
func (t *Topic) GetSubscriptions() ([]*Subscription, error) {
	return globalSubscription.CollectByTopicID(t.Name)
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
