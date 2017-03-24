package queue

import (
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Topic errors
var (
	ErrAlreadyExistTopic = errors.New("already exist topic")
	ErrNotFoundTopic     = errors.New("not found topic")
)

// Global variable Topic map
var GlobalTopics *topics = newTopics()

type topics struct {
	topics map[string]*Topic
	mu     sync.Mutex
}

func newTopics() *topics {
	return &topics{
		topics: make(map[string]*Topic),
	}
}

func (ts *topics) Get(key string) (*Topic, bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	t, ok := ts.topics[key]
	return t, ok
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

type Topic struct {
	name          string
	store         Datastore
	subscriptions []Subscription
}

// Create topic, if not exist already topic name in GlobalTopics
func NewTopic(name string, store Datastore) (*Topic, error) {
	if _, ok := GlobalTopics.Get(name); ok {
		return nil, ErrAlreadyExistTopic
	}

	t := &Topic{
		name:  name,
		store: store,
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

// Delete topic object at GlobalTopics
func (t *Topic) Delete() {
	GlobalTopics.Delete(t.name)
}

// Message store backend storage and delivery to Subscription
func (t *Topic) Publish(data []byte, attributes map[string]string) error {
	acks := &sendSubscriptions{
		list: make(map[string]bool),
	}
	for _, sub := range t.subscriptions {
		acks.add(sub.name)
	}

	m := Message{
		ID:          makeMessageID(),
		Data:        data,
		Attributes:  newAttributes(attributes),
		sends:       acks,
		PublishedAt: time.Now(),
	}
	err := t.store.Set(m)
	if err != nil {
		return errors.Wrapf(err, "failed store message")
	}

	for _, s := range t.subscriptions {
		// TODO: need pointer?
		s.Subscribe(&m)
	}
	return nil
}
