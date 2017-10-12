package models

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/pkg/errors"
	"github.com/takashabe/go-pubsub/datastore"
)

// globalMessageStatus global subscription datastore
var (
	globalMessageStatus   *DatastoreMessageStatus
	globalMessageStatusMu sync.RWMutex
)

func getGlobalMessageStatus() *DatastoreMessageStatus {
	globalMessageStatusMu.RLock()
	defer globalMessageStatusMu.RUnlock()
	return globalMessageStatus
}

func setGlobalMessageStatus(v *DatastoreMessageStatus) {
	globalMessageStatusMu.Lock()
	defer globalMessageStatusMu.Unlock()
	globalMessageStatus = v
}

// DatastoreMessageStatus is adapter between actual datastore and datastore client
type DatastoreMessageStatus struct {
	store datastore.Datastore
}

// NewDatastoreMessageStatus create DatastoreTopic object
func NewDatastoreMessageStatus(cfg *datastore.Config) (*DatastoreMessageStatus, error) {
	d, err := datastore.LoadDatastore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load datastore")
	}
	return &DatastoreMessageStatus{
		store: d,
	}, nil
}

// InitDatastoreMessageStatus initialize global datastore object
func InitDatastoreMessageStatus() error {
	d, err := NewDatastoreMessageStatus(datastore.GlobalConfig)
	if err != nil {
		return err
	}
	setGlobalMessageStatus(d)
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

// Get return item via datastore
func (d *DatastoreMessageStatus) Get(key string) (*MessageStatus, error) {
	v, err := d.store.Get(d.prefix(key))
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

// ListBySubscriptionID return all MessageStatus slice matched SubscriptionID
func (d *DatastoreMessageStatus) ListBySubscriptionID(subID string) ([]*MessageStatus, error) {
	return d.collectByField(func(ms *MessageStatus) bool {
		return ms.SubscriptionID == subID
	})
}

// Set save item to datastore
func (d *DatastoreMessageStatus) Set(ms *MessageStatus) error {
	v, err := datastore.EncodeGob(ms)
	if err != nil {
		return err
	}
	return d.store.Set(d.prefix(ms.ID), v)
}

// Delete delete item
func (d *DatastoreMessageStatus) Delete(key string) error {
	return d.store.Delete(d.prefix(key))
}

// CollectByIDs returns all MessageStatus depends ids
func (d *DatastoreMessageStatus) CollectByIDs(ids ...string) ([]*MessageStatus, error) {
	return d.collectByField(func(ms *MessageStatus) bool {
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
	sources, err := datastore.SpecifyDump(d.store, d.prefix(""))
	if err != nil {
		return nil, err
	}
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
	sources, err := datastore.SpecifyDump(d.store, d.prefix(""))
	if err != nil {
		return nil, err
	}
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

func (d *DatastoreMessageStatus) prefix(key string) string {
	return "message_status_" + key
}
