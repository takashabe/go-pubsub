package models

import "github.com/pkg/errors"

// globalMessage Global message key-value store object
var globalMessage *DatastoreMessage

// DatastoreMessage is adapter between actual datastore and datastore client
type DatastoreMessage struct {
	store Datastore
}

// NewDatastoreMessage create DatastoreTopic object
func NewDatastoreMessage(cfg *Config) (*DatastoreMessage, error) {
	d, err := LoadDatastore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load datastore")
	}
	return &DatastoreMessage{
		store: d,
	}, nil
}

// InitDatastoreMessage initialize global datastore object
func InitDatastoreMessage() error {
	d, err := NewDatastoreMessage(globalConfig)
	if err != nil {
		return err
	}
	globalMessage = d
	return nil
}

func (m *DatastoreMessage) Get(key string) (*Message, error) {
	t, err := m.store.Get(key)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, ErrNotFoundTopic
	}

	v, ok := t.(*Message)
	if !ok {
		return nil, errors.Wrapf(ErrNotMatchTypeMessage, "key=%s", key)
	}
	return v, nil
}

func (m *DatastoreMessage) List() ([]*Message, error) {
	values := m.store.Dump()
	res := make([]*Message, 0, len(values))
	for k, v := range values {
		if vt, ok := v.(*Message); ok {
			res = append(res, vt)
		} else {
			return nil, errors.Wrapf(ErrNotMatchTypeMessage, "key=%s", k)
		}
	}
	return res, nil
}

func (m *DatastoreMessage) Set(v *Message) error {
	return m.store.Set(v.ID, v)
}

func (m *DatastoreMessage) Delete(key string) error {
	return m.store.Delete(key)
}

func (m *DatastoreMessage) Size() int {
	return len(m.store.Dump())
}
