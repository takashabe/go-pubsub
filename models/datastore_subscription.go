package models

import "github.com/pkg/errors"

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

func (ts *DatastoreSubscription) Get(key string) (*Subscription, error) {
	t := ts.store.Get(key)
	if t == nil {
		return nil, errors.Wrapf(ErrNotFoundSubscription, "key=%s", key)
	}
	v, ok := t.(*Subscription)
	if !ok {
		return nil, errors.Wrapf(ErrNotMatchTypeSubscription, "key=%s", key)
	}
	return v, nil
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
	for k, v := range values {
		if vt, ok := v.(*Subscription); ok {
			res = append(res, vt)
		} else {
			return nil, errors.Wrapf(ErrNotMatchTypeSubscription, "key=%s", k)
		}
	}
	return res, nil
}

func (ts *DatastoreSubscription) Set(sub *Subscription) error {
	return ts.store.Set(sub.Name, sub)
}

func (ts *DatastoreSubscription) Delete(key string) error {
	return ts.store.Delete(key)
}
