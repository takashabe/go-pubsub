package models

import (
	"bytes"
	"encoding/gob"

	"github.com/pkg/errors"
)

var globalMessageStatus *DatastoreMessageStatus

// DatastoreMessageStatus is adapter between actual datastore and datastore client
type DatastoreMessageStatus struct {
	store Datastore
}

// NewDatastoreMessageStatus create DatastoreTopic object
func NewDatastoreMessageStatus(cfg *Config) (*DatastoreMessageStatus, error) {
	d, err := LoadDatastore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load datastore")
	}
	return &DatastoreMessageStatus{
		store: d,
	}, nil
}

// InitDatastoreMessageStatus initialize global datastore object
func InitDatastoreMessageStatus() error {
	d, err := NewDatastoreMessageStatus(globalConfig)
	if err != nil {
		return err
	}
	globalMessageStatus = d
	return nil
}

func decodeRawMessageStatus(r interface{}) (*MessageStatus, error) {
	switch a := r.(type) {
	case []byte:
		return decodeGobMessageStatus(a)
	default:
		return nil, ErrNotMatchTypeTopic
	}
}

func decodeGobMessageStatus(e []byte) (*MessageStatus, error) {
	var res *MessageStatus
	buf := bytes.NewReader(e)
	if err := gob.NewDecoder(buf).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

func (d *DatastoreMessageStatus) Get(key string) (*MessageStatus, error) {
	v, err := d.store.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrNotFoundEntry
	}
	return decodeRawMessageStatus(v)
}

// FindByMessageID return MessageStatus matched MessageID
func (d *DatastoreMessageStatus) FindByMessageID(msgID string) (*MessageStatus, error) {
	switch d.store.(type) {
	case *Memory, *Redis, *MySQL:
		values := d.store.Dump()
		for _, v := range values {
			m, err := decodeRawMessageStatus(v)
			if err != nil {
				return nil, err
			}
			if m.MessageID == msgID {
				return m, nil
			}
		}
		return nil, ErrNotFoundEntry
	default:
		return nil, ErrNotSupportOperation
	}
}

// FindByAckID return MessageStatus matched AckID
func (d *DatastoreMessageStatus) FindByAckID(ackID string) (*MessageStatus, error) {
	switch d.store.(type) {
	case *Memory, *Redis, *MySQL:
		values := d.store.Dump()
		for _, v := range values {
			m, err := decodeRawMessageStatus(v)
			if err != nil {
				return nil, err
			}
			if m.AckID == ackID {
				return m, nil
			}
		}
		return nil, ErrNotFoundEntry
	default:
		return nil, ErrNotSupportOperation
	}
}

// List return all MessageStatus slice
func (d *DatastoreMessageStatus) List() ([]*MessageStatus, error) {
	values := d.store.Dump()
	res := make([]*MessageStatus, 0, len(values))
	for _, v := range values {
		m, err := decodeRawMessageStatus(v)
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, nil
}

func (d *DatastoreMessageStatus) Set(ms *MessageStatus) error {
	v, err := EncodeGob(ms)
	if err != nil {
		return err
	}
	return d.store.Set(ms.ID, v)
}

func (m *DatastoreMessageStatus) Delete(key string) error {
	return m.store.Delete(key)
}

func (m *DatastoreMessageStatus) Size() int {
	return len(m.store.Dump())
}

// CollectByReadable return MessageStatus slice matched readable message
func (m *DatastoreMessageStatus) CollectByReadableMessage(size int) ([]*Message, error) {
	switch m.store.(type) {
	case *Memory:
		source := m.store.Dump()
		dst := make([]*Message, 0)
		for k, v := range source {
			if len(dst) >= size {
				return dst, nil
			}
			ms, ok := v.(*MessageStatus)
			if !ok {
				return nil, errors.Wrapf(ErrNotMatchTypeMessageStatus, "key=%s", k)
			}
			if !ms.Readable() {
				continue
			}
			if msg, err := globalMessage.Get(ms.MessageID); err == nil {
				dst = append(dst, msg)
			}
		}
		return dst, nil
	case *MySQL:
		return nil, ErrNotSupportOperation
	default:
		return nil, ErrNotSupportOperation
	}
}
