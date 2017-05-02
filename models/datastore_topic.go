package models

import (
	"bytes"
	"encoding/gob"

	"github.com/pkg/errors"
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
	v, err := ts.store.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrNotFoundEntry
	}
	return decodeRawTopic(v)
}

func (ts *DatastoreTopic) List() ([]*Topic, error) {
	values := ts.store.Dump()
	res := make([]*Topic, 0, len(values))
	for _, v := range values {
		t, err := decodeRawTopic(v)
		if err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, nil
}

func (ts *DatastoreTopic) Set(topic *Topic) error {
	v, err := EncodeGob(topic)
	if err != nil {
		return err
	}
	return ts.store.Set(topic.Name, v)
}

func (ts *DatastoreTopic) Delete(key string) error {
	return ts.store.Delete(key)
}
