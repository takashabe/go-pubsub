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
		return nil, ErrNotMatchTypeMessageStatus
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

// FindBySubscriptionIDAndMessageID return MessageStatus matched MessageID
func (d *DatastoreMessageStatus) FindBySubscriptionIDAndMessageID(subID, msgID string) (*MessageStatus, error) {
	return d.chooseByField(func(ms *MessageStatus) bool {
		return ms.SubscriptionID == subID && ms.MessageID == msgID
	})
}

// FindByAckID return MessageStatus matched AckID
func (d *DatastoreMessageStatus) FindByAckID(ackID string) (*MessageStatus, error) {
	return d.chooseByField(func(ms *MessageStatus) bool {
		return ms.AckID == ackID
	})
}

// List return all MessageStatus slice
func (d *DatastoreMessageStatus) List() ([]*MessageStatus, error) {
	return d.collectByField(func(ms *MessageStatus) bool {
		return true
	})
}

func (d *DatastoreMessageStatus) Set(ms *MessageStatus) error {
	v, err := EncodeGob(ms)
	if err != nil {
		return err
	}
	return d.store.Set(ms.ID, v)
}

func (d *DatastoreMessageStatus) Delete(key string) error {
	return d.store.Delete(key)
}

func (d *DatastoreMessageStatus) Size() int {
	return len(d.store.Dump())
}

// SelectByIDs returns all MessageStatus depends ids
func (d *DatastoreMessageStatus) CollectByIDs(ids ...string) ([]*MessageStatus, error) {
	return d.collectByField(func(ms *MessageStatus) bool {
		// TODO: improve performance
		for _, id := range ids {
			if ms.ID == id {
				return true
			}
		}
		return false
	})
}

// chooseByField choose any matched a MessageStatus
func (d *DatastoreMessageStatus) chooseByField(fn func(ms *MessageStatus) bool) (*MessageStatus, error) {
	sources := d.store.Dump()
	for _, v := range sources {
		ms, err := decodeRawMessageStatus(v)
		if err != nil {
			return nil, err
		}
		if fn(ms) {
			return ms, nil
		}
	}
	return nil, ErrNotFoundEntry
}

// collectByField collect any matched MessageStatus list
func (d *DatastoreMessageStatus) collectByField(fn func(ms *MessageStatus) bool) ([]*MessageStatus, error) {
	sources := d.store.Dump()
	res := make([]*MessageStatus, 0, len(sources))
	for _, v := range sources {
		ms, err := decodeRawMessageStatus(v)
		if err != nil {
			return nil, err
		}
		if fn(ms) {
			res = append(res, ms)
		}
	}
	return res, nil
}
