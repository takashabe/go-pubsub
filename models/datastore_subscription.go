package models

import (
	"bytes"
	"encoding/gob"

	"github.com/pkg/errors"
)

// globalSubscription global subscription datastore
var globalSubscription *DatastoreSubscription

// DatastoreSubscription is adapter between actual datastore and datastore client
type DatastoreSubscription struct {
	store Datastore
}

// NewDatastoreSubscription create DatastoreSubscription object
func NewDatastoreSubscription(cfg *Config) (*DatastoreSubscription, error) {
	d, err := LoadDatastore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load datastore")
	}
	return &DatastoreSubscription{
		store: d,
	}, nil
}

// InitDatastoreSubscription initialize global datastore object
func InitDatastoreSubscription() error {
	d, err := NewDatastoreSubscription(globalConfig)
	if err != nil {
		return err
	}
	globalSubscription = d
	return nil
}

func decodeRawSubscription(r interface{}) (*Subscription, error) {
	switch a := r.(type) {
	case []byte:
		return decodeGobSubscription(a)
	default:
		return nil, ErrNotMatchTypeSubscription
	}
}

func decodeGobSubscription(e []byte) (*Subscription, error) {
	var res *Subscription
	buf := bytes.NewReader(e)
	if err := gob.NewDecoder(buf).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

func (ts *DatastoreSubscription) Get(key string) (*Subscription, error) {
	v, err := ts.store.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrNotFoundEntry
	}
	return decodeRawSubscription(v)
}

func (ts *DatastoreSubscription) CollectByTopicID(topicID string) ([]*Subscription, error) {
	switch ts.store.(type) {
	case *Memory, *Redis, *MySQL:
		res := make([]*Subscription, 0)
		for _, v := range ts.store.Dump() {
			s, ok := v.(*Subscription)
			if !ok {
				return nil, errors.Wrapf(ErrNotMatchTypeSubscription, "key=%s", topicID)
			}
			if s.TopicID != topicID {
				continue
			}
			res = append(res, s)
		}
		return res, nil
	default:
		return nil, ErrNotSupportOperation
	}
}

func (ts *DatastoreSubscription) List() ([]*Subscription, error) {
	values := ts.store.Dump()
	res := make([]*Subscription, 0, len(values))
	for _, v := range values {
		t, err := decodeRawSubscription(v)
		if err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, nil
}

func (ts *DatastoreSubscription) Set(sub *Subscription) error {
	v, err := EncodeGob(sub)
	if err != nil {
		return errors.Wrapf(err, "failed to encode gob")
	}
	return ts.store.Set(sub.Name, v)
}

func (ts *DatastoreSubscription) Delete(key string) error {
	return ts.store.Delete(key)
}
