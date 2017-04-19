package models

import (
	"io/ioutil"
	"log"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/k0kubun/pp"
	"github.com/pkg/errors"
)

// TODO: initialize config from Server
var globalConfig *Config

// Config is connect to datastore parameters
type Config struct {
	Driver   string
	Endpoint string

	// username and password must be set by env
	UsernameEnv string
	PasswordEnv string
}

// LoadConfigFromFile read config file and create config object
func LoadConfigFromFile(path string) (*Config, error) {
	_, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// TODO: impl read yaml and mapping struct
	return &Config{Driver: "memory"}, nil
}

// SetGlobalConfig set global config
func SetGlobalConfig(cfg *Config) {
	globalConfig = cfg
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

type Redis struct {
	conn redis.Conn
}

// NewRedis return redis client
func NewRedis(cfg *Config) (*Redis, error) {
	conn, err := redis.Dial("tcp", cfg.Endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect redis")
	}
	return &Redis{conn: conn}, nil
}

func (r *Redis) Set(key, value interface{}) error {
	_, err := r.conn.Do("SET", key, value)
	return err
}

func (r *Redis) Get(key interface{}) interface{} {
	v, err := r.conn.Do("GET", key)
	if err != nil {
		log.Printf("[REDIS] failed to get, key=%s, err=%v", key, err)
		return nil
	}
	return v
}

func (r *Redis) Delete(key interface{}) error {
	_, err := r.conn.Do("DEL", key)
	return err
}

func (r *Redis) Dump() map[interface{}]interface{} {
	res := make(map[interface{}]interface{})
	keys, err := redis.Strings(r.conn.Do("KEYS", "*"))
	if err != nil {
		return res
	}
	args := make([]interface{}, 0)
	for _, k := range keys {
		args = append(args, k)
	}
	values, err := r.conn.Do("MGET", args...)
	pp.Println(values)
	if err != nil {
		return res
	}
	return res
}

// TODO: impl datastore
type MySQL struct{}

func NewMySQL(cfg *Config) (*MySQL, error)         { return nil, nil }
func (m *MySQL) Set(key, value interface{}) error  { return nil }
func (m *MySQL) Get(key interface{}) interface{}   { return nil }
func (m *MySQL) Delete(key interface{}) error      { return nil }
func (m *MySQL) Dump() map[interface{}]interface{} { return nil }
