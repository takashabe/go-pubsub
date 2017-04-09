package models

import (
	"io/ioutil"
	"sync"
)

// Config is connect to datastore parameters
type Config struct {
	Driver   string
	Endpoint string

	// username and password must be set by env
	UsernameEnv string
	PasswordEnv string
}

// LoadConfigFromFile read config file and create config object
func LoadConfigFromFile(driver string, path string) (*Config, error) {
	_, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// Datastore is behavior like Key-Value store
type Datastore interface {
	Set(key, value interface{}) error
	Get(key interface{}) interface{}
	Delete(key interface{}) error
	Dump() map[interface{}]interface{}
}

// Load backend datastore from cnofiguration json file.
func LoadDatastore(cfg *Config) (Datastore, error) {
	switch cfg.Driver {
	case "memory":
		return NewMemory(cfg), nil
	case "mysql":
		return NewMySQL(cfg)
	default:
		return NewMemory(cfg), nil
	}
}

// Datastore driver at "in memory"
type Memory struct {
	store map[interface{}]interface{}
	mu    sync.RWMutex
}

// Create memory object
func NewMemory(_ *Config) *Memory {
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
func (m *Memory) Get(key interface{}) interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.store[key]
}

// Delete item
func (m *Memory) Delete(key interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.store, key)
	return nil
}

// Dump store values
func (m *Memory) Dump() map[interface{}]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.store
}

// TODO: impl datastore
type MySQL struct{}

func NewMySQL(cfg *Config) (*MySQL, error)         { return nil, nil }
func (m *MySQL) Set(key, value interface{}) error  { return nil }
func (m *MySQL) Get(key interface{}) interface{}   { return nil }
func (m *MySQL) Delete(key interface{}) error      { return nil }
func (m *MySQL) Dump() map[interface{}]interface{} { return nil }
