package queue

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/pkg/errors"
)

// Datastore errors
var (
	ErrNotFoundMessage = errors.New("not found entry")
)

// Datastore is behavior like Key-Value store
type Datastore interface {
	Set(key, value interface{}) error
	Get(key interface{}) (interface{}, error)
	Delete(key interface{}) error
}

// Load backend datastore from cnofiguration json file.
func LoadDatastore(path string) (Datastore, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// TODO: loading json file and create datastore object
	fmt.Println(string(data))

	return nil, nil
}

// Datastore driver at "in memory"
type Memory struct {
	store map[interface{}]interface{}
	mu    sync.RWMutex
}

// Create memory object
func NewMemory() *Memory {
	return &Memory{
		store: make(map[interface{}]interface{}),
	}
}

// Save item
func (m *Memory) Set(key, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store[key] = value
	return nil
}

// Get item
func (m *Memory) Get(key interface{}) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item, ok := m.store[key]
	if !ok {
		return nil, errors.Wrapf(ErrNotFoundMessage, fmt.Sprintf("key=%s", key))
	}
	return item, nil
}

// Delete item
func (m *Memory) Delete(key interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.store, key)
	return nil
}
