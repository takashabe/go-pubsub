package queue

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

// Topic errors
var (
	ErrAlreadyExistTopic        = errors.New("already exist topic")
	ErrAlreadyExistSubscription = errors.New("already exist subscription")
	ErrNotFoundTopic            = errors.New("not found topic")
)

// Global variable Topic map
var GlobalTopics *topics = newTopics()

type topics struct {
	topics map[string]*Topic
	mu     sync.RWMutex
}

func newTopics() *topics {
	return &topics{
		topics: make(map[string]*Topic),
	}
}

func (ts *topics) Get(key string) (*Topic, bool) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	t, ok := ts.topics[key]
	return t, ok
}

func (ts *topics) List() map[string]*Topic {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.topics
}

func (ts *topics) Set(topic *Topic) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.topics[topic.name] = topic
}

func (ts *topics) Delete(key string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	delete(ts.topics, key)
}

// Topic object
type Topic struct {
	name          string
	store         Datastore
	subscriptions map[string]Subscription
	mu            sync.RWMutex
}

// Create topic, if not exist already topic name in GlobalTopics
func NewTopic(name string, store Datastore) (*Topic, error) {
	if _, ok := GlobalTopics.Get(name); ok {
		return nil, ErrAlreadyExistTopic
	}

	t := &Topic{
		name:          name,
		store:         store,
		subscriptions: make(map[string]Subscription),
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
func ListTopic() (map[string]*Topic, error) {
	list := GlobalTopics.List()
	if len(list) == 0 {
		return nil, ErrNotFoundTopic
	}
	return list, nil
}

// Delete topic object at GlobalTopics
func (t *Topic) Delete() {
	GlobalTopics.Delete(t.name)
}

// Register subscription
func (t *Topic) AddSubscription(s Subscription) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.subscriptions[s.name]; ok {
		return errors.Wrapf(ErrAlreadyExistSubscription, fmt.Sprintf("id=%s", s.name))
	}
	t.subscriptions[s.name] = s
	return nil
}

// Message store backend storage and delivery to Subscription
func (t *Topic) Publish(data []byte, attributes map[string]string) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	m := NewMessage(makeMessageID(), *t, data, attributes, t.subscriptions)
	err := t.store.Set(m.ID, m)
	if err != nil {
		return errors.Wrapf(err, "failed store message")
	}

	for _, s := range t.subscriptions {
		s.Subscribe(m)
	}
	return nil
}
