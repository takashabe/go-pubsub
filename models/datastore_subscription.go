package queue

import "github.com/pkg/errors"

type DatastoreSubscription struct {
	store Datastore
}

func NewDatastoreSubscription() *DatastoreSubscription {
	// TODO: flexible datastore source
	return &DatastoreSubscription{
		store: NewMemory(),
	}
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
	return ts.store.Set(sub.name, sub)
}

func (ts *DatastoreSubscription) Delete(key string) error {
	return ts.store.Delete(key)
}
