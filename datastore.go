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

type Datastore interface {
	Set(m Message) error
	Get(id string) (Message, error)
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
	messages map[string]Message
	mu       sync.Mutex
}

// Create memory object
func NewMemory() *Memory {
	return &Memory{
		messages: make(map[string]Message),
	}
}

// Save message to memory
func (m *Memory) Set(item Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messages[item.ID] = item
	return nil
}

// Get message from memory
func (m *Memory) Get(id string) (Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	item, ok := m.messages[id]
	if !ok {
		return Message{}, errors.Wrapf(ErrNotFoundMessage, fmt.Sprintf("id=%s", id))
	}
	return item, nil
}
