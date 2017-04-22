package models

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/k0kubun/pp"
	"github.com/pkg/errors"
)

// Datastore is behavior like Key-Value store
type Datastore interface {
	Set(key, value interface{}) error
	Get(key interface{}) interface{}
	Delete(key interface{}) error
	Dump() map[interface{}]interface{}
}

// Load backend datastore from cnofiguration json file.
func LoadDatastore(cfg *Config) (Datastore, error) {
	if cfg == nil {
		return NewMemory(nil), nil
	}

	if cfg.Datastore.Redis != nil {
		return NewRedis(cfg)
	}
	if cfg.Datastore.MySQL != nil {
		return nil, ErrNotSupportDriver
	}
	return NewMemory(nil), nil
}

func EncodeGob(s interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeGobMessage(e []byte) (*Message, error) {
	var res *Message
	buf := bytes.NewReader(e)
	if err := gob.NewDecoder(buf).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

// Datastore driver at "in memory"
type Memory struct {
	Store map[interface{}]interface{}
	mu    sync.RWMutex
}

// Create memory object
func NewMemory(_ *Config) *Memory {
	return &Memory{
		Store: make(map[interface{}]interface{}),
	}
}

// Save item
func (m *Memory) Set(key, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Store[key] = value
	return nil
}

// Get item
func (m *Memory) Get(key interface{}) interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.Store[key]
}

// Delete item
func (m *Memory) Delete(key interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.Store, key)
	return nil
}

// Dump store values
func (m *Memory) Dump() map[interface{}]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.Store
}

type Redis struct {
	conn redis.Conn
}

// NewRedis return redis client
func NewRedis(cfg *Config) (*Redis, error) {
	rconf := cfg.Datastore.Redis
	endpoint := fmt.Sprintf("%s:%d", rconf.Host, rconf.Port)
	conn, err := redis.Dial("tcp", endpoint)
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
