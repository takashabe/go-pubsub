package models

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"sync"

	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

// Datastore is behavior like Key-Value store
type Datastore interface {
	Set(key, value interface{}) error
	Get(key interface{}) (interface{}, error)
	Delete(key interface{}) error
	Dump() (map[interface{}]interface{}, error)
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
		return NewMySQL(cfg)
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

// SpecifyDump return Dump entries, each Datastore type
func SpecifyDump(d Datastore, key string) (map[interface{}]interface{}, error) {
	// TODO: SpecifyDump is used to acquire entries for each type, but it is more efficient to divide the DB/Table.
	var res map[interface{}]interface{}
	switch a := d.(type) {
	case *Redis:
		v, err := a.DumpPrefix(key)
		if err != nil {
			return nil, err
		}
		res = v
	case *MySQL:
		v, err := a.DumpPrefix(key)
		if err != nil {
			return nil, err
		}
		res = v
	default:
		v, err := a.Dump()
		if err != nil {
			return nil, err
		}
		res = v
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
func (m *Memory) Get(key interface{}) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.Store[key]
	if !ok {
		return nil, ErrNotFoundEntry
	}
	return v, nil
}

// Delete item
func (m *Memory) Delete(key interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.Store, key)
	return nil
}

// Dump store values
func (m *Memory) Dump() (map[interface{}]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.Store, nil
}

type Redis struct {
	Conn redis.Conn
}

// NewRedis return redis client
func NewRedis(cfg *Config) (*Redis, error) {
	rconf := cfg.Datastore.Redis
	endpoint := fmt.Sprintf("%s:%d", rconf.Host, rconf.Port)
	conn, err := redis.Dial("tcp", endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect redis")
	}
	return &Redis{Conn: conn}, nil
}

// FlushDB delete all current DB on Redis
func (r *Redis) FlushDB() error {
	log.Println("EXECUTE FLUSHDB...")
	_, err := r.Conn.Do("FLUSHDB")
	return err
}

func (r *Redis) Set(key, value interface{}) error {
	_, err := r.Conn.Do("SET", key, value)
	return err
}

func (r *Redis) Get(key interface{}) (interface{}, error) {
	v, err := redis.Bytes(r.Conn.Do("GET", key))
	if err != nil {
		return nil, errors.Wrapf(ErrNotFoundEntry, fmt.Sprintf("detail %v", err))
	}
	return v, nil
}

func (r *Redis) Delete(key interface{}) error {
	_, err := r.Conn.Do("DEL", key)
	return err
}

func (r *Redis) Dump() (map[interface{}]interface{}, error) {
	return r.DumpPrefix("")
}

func (r *Redis) DumpPrefix(p string) (map[interface{}]interface{}, error) {
	// get keys
	keys, err := redis.Strings(r.Conn.Do("KEYS", p+"*"))
	if err != nil {
		return nil, err
	}
	args := make([]interface{}, 0)
	for _, k := range keys {
		args = append(args, k)
	}

	// get values
	values, err := redis.ByteSlices(r.Conn.Do("MGET", args...))
	if err != nil {
		return nil, err
	}

	// key-valus to map
	res := make(map[interface{}]interface{}, len(keys))
	for i, k := range keys {
		res[k] = values[i]
	}
	return res, nil
}

// MySQL is MySQL datastore driver
type MySQL struct {
	Conn *sql.DB
}

type generalSchema struct {
	id    string
	value []byte
}

// NewMySQL return MySQL client
func NewMySQL(cfg *Config) (*MySQL, error) {
	c := cfg.Datastore.MySQL
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/mq", c.User, c.Password, c.Host, c.Port))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect mysql")
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &MySQL{
		Conn: db,
	}, nil
}

func (m *MySQL) Set(key, value interface{}) error {
	stmt, err := m.Conn.Prepare(`INSERT INTO mq (id, value) VALUES (?, ?) 
		ON DUPLICATE KEY UPDATE value=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if _, err := stmt.Exec(key, value, value); err != nil {
		return err
	}
	return nil
}

func (m *MySQL) Get(key interface{}) (interface{}, error) {
	stmt, err := m.Conn.Prepare("SELECT value FROM mq WHERE id=?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var s generalSchema
	if err := stmt.QueryRow(key).Scan(&s.value); err != nil {
		return nil, errors.Wrapf(ErrNotFoundEntry, fmt.Sprintf("detail %v", err))
	}
	return s.value, nil
}

func (m *MySQL) Delete(key interface{}) error {
	stmt, err := m.Conn.Prepare("DELETE FROM mq WHERE id=?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	if _, err := stmt.Exec(key); err != nil {
		return err
	}
	return nil
}

func (m *MySQL) Dump() (map[interface{}]interface{}, error) {
	rows, err := m.Conn.Query("SELECT id, value FROM mq")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return m.convertRowsToMap(rows)
}

func (m *MySQL) DumpPrefix(p string) (map[interface{}]interface{}, error) {
	stmt, err := m.Conn.Prepare("SELECT id, value FROM mq WHERE id like ?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(p + "%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return m.convertRowsToMap(rows)
}

func (m *MySQL) convertRowsToMap(rows *sql.Rows) (map[interface{}]interface{}, error) {
	res := make(map[interface{}]interface{}, 0)
	for rows.Next() {
		var s generalSchema
		if err := rows.Scan(&s.id, &s.value); err != nil {
			return nil, err
		}
		res[s.id] = s.value
	}
	return res, nil
}
