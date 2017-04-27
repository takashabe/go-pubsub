package models

import "github.com/pkg/errors"
import (
	"bytes"
	"encoding/gob"

)

// globalTopics global Topic datastore
var globalTopics *DatastoreTopic

// DatastoreTopic is adapter between actual datastore and datastore client
type DatastoreTopic struct {
	store Datastore
}

// NewDatastoreTopic create DatastoreTopic object
func NewDatastoreTopic(cfg *Config) (*DatastoreTopic, error) {
	d, err := LoadDatastore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load datastore")
	}
	return &DatastoreTopic{
		store: d,
	}, nil
}

// InitDatastoreTopic initialize global datastore object
func InitDatastoreTopic() error {
	d, err := NewDatastoreTopic(globalConfig)
	if err != nil {
		return err
	}
	globalTopics = d
	return nil
}

func decodeRawTopic(r interface{}) (*Topic, error) {
	switch a := r.(type) {
	case *Topic:
		return a, nil
	case []byte:
		return decodeGobTopic(a)
	default:
		return nil, ErrNotMatchTypeTopic
	}
}

func decodeGobTopic(e []byte) (*Topic, error) {
	var res *Topic
	buf := bytes.NewReader(e)
	if err := gob.NewDecoder(buf).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

func (ts *DatastoreTopic) Get(key string) (*Topic, error) {
	t := ts.store.Get(key)
	if t == nil {
		return nil, errors.Wrapf(ErrNotFoundTopic, "key=%s", key)
	}
	v, ok := t.(*Topic)
	if !ok {
		return nil, errors.Wrapf(ErrNotMatchTypeTopic, "key=%s", key)
	}
	return v, nil
}

func (ts *DatastoreTopic) List() ([]*Topic, error) {
	values := ts.store.Dump()
	res := make([]*Topic, 0, len(values))
	for _, v := range values {
		if vt, ok := v.(*Topic); ok {
			res = append(res, vt)
		} else {
			return nil, ErrNotMatchTypeTopic
		}
	}
	return res, nil
}

func (ts *DatastoreTopic) Set(topic *Topic) error {
	return ts.store.Set(topic.Name, topic)
}

func (ts *DatastoreTopic) Delete(key string) error {
	return ts.store.Delete(key)
}
