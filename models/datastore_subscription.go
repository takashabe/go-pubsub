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

func (d *DatastoreSubscription) CollectByTopicID(topicID string) ([]*Subscription, error) {
	return d.collectByField(func(s *Subscription) bool {
		return s.TopicID == topicID
	})
}

func (d *DatastoreSubscription) List() ([]*Subscription, error) {
	return d.collectByField(func(s *Subscription) bool {
		return true
	})
}

// chooseByField choose any matched a Subscription
func (d *DatastoreSubscription) chooseByField(fn func(ms *Subscription) bool) (*Subscription, error) {
	sources := d.store.Dump()
	for _, v := range sources {
		ms, err := decodeRawSubscription(v)
		if err != nil {
			return nil, err
		}
		if fn(ms) {
			return ms, nil
		}
	}
	return nil, ErrNotFoundEntry
}

// collectByField collect any matched Subscription list
func (d *DatastoreSubscription) collectByField(fn func(ms *Subscription) bool) ([]*Subscription, error) {
	sources := d.store.Dump()
	res := make([]*Subscription, 0, len(sources))
	for _, v := range sources {
		ms, err := decodeRawSubscription(v)
		if err != nil {
			return nil, err
		}
		if fn(ms) {
			res = append(res, ms)
		}
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
