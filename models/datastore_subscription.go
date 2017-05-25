package models

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/pkg/errors"
	"github.com/takashabe/go-message-queue/datastore"
)

// globalSubscription global subscription datastore
var (
	globalSubscription   *DatastoreSubscription
	globalSubscriptionMu sync.RWMutex
)

func getGlobalSubscription() *DatastoreSubscription {
	globalSubscriptionMu.RLock()
	defer globalSubscriptionMu.RUnlock()
	return globalSubscription
}

func setGlobalSubscription(v *DatastoreSubscription) {
	globalSubscriptionMu.Lock()
	defer globalSubscriptionMu.Unlock()
	globalSubscription = v
}

// DatastoreSubscription is adapter between actual datastore and datastore client
type DatastoreSubscription struct {
	store datastore.Datastore
}

// NewDatastoreSubscription create DatastoreSubscription object
func NewDatastoreSubscription(cfg *datastore.Config) (*DatastoreSubscription, error) {
	d, err := datastore.LoadDatastore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load datastore")
	}
	return &DatastoreSubscription{
		store: d,
	}, nil
}

// InitDatastoreSubscription initialize global datastore object
func InitDatastoreSubscription() error {
	d, err := NewDatastoreSubscription(datastore.GlobalConfig)
	if err != nil {
		return err
	}
	setGlobalSubscription(d)
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

func (d *DatastoreSubscription) Get(key string) (*Subscription, error) {
	v, err := d.store.Get(d.prefix(key))
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
	sources, err := datastore.SpecifyDump(d.store, d.prefix(""))
	if err != nil {
		return nil, err
	}
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
	sources, err := datastore.SpecifyDump(d.store, d.prefix(""))
	if err != nil {
		return nil, err
	}
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

func (d *DatastoreSubscription) Set(sub *Subscription) error {
	v, err := datastore.EncodeGob(sub)
	if err != nil {
		return errors.Wrapf(err, "failed to encode gob")
	}
	return d.store.Set(d.prefix(sub.Name), v)
}

func (d *DatastoreSubscription) Delete(key string) error {
	return d.store.Delete(d.prefix(key))
}

func (d *DatastoreSubscription) prefix(key string) string {
	return "subscription_" + key
}
