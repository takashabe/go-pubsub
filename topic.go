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
	ErrInvalidTopic             = errors.New("invalid topic")
)

// Global variable Topic map
var GlobalTopics *topics = newTopics()

type topics struct {
	store  Datastore
	topics map[string]*Topic
	mu     sync.RWMutex
}

func newTopics() *topics {
	return &topics{
		store: NewMemory(),
	}
}

func (ts *topics) Get(key string) (*Topic, bool) {
	t := ts.store.Get(key)
	if v, ok := t.(*Topic); ok {
		return v, true
	}
	return nil, false
}

func (ts *topics) List() ([]*Topic, error) {
	values := ts.store.Dump()
	res := make([]*Topic, 0, len(values))
	for _, v := range values {
		if vt, ok := v.(*Topic); ok {
			res = append(res, vt)
		} else {
			return nil, ErrInvalidTopic
		}
	}
	return res, nil
}

func (ts *topics) Set(topic *Topic) error {
	return ts.store.Set(topic.name, topic)
}

func (ts *topics) Delete(key string) error {
	return ts.store.Delete(key)
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
func ListTopic() ([]*Topic, error) {
	return GlobalTopics.List()
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
