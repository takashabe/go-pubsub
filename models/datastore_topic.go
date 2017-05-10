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

func (d *DatastoreTopic) Get(key string) (*Topic, error) {
	v, err := d.store.Get(d.prefix(key))
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrNotFoundEntry
	}
	return decodeRawTopic(v)
}

func (d *DatastoreTopic) List() ([]*Topic, error) {
	sources, err := SpecifyDump(d.store, d.prefix(""))
	if err != nil {
		return nil, err
	}
	res := make([]*Topic, 0, len(sources))
	for _, v := range sources {
		t, err := decodeRawTopic(v)
		if err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, nil
}

func (d *DatastoreTopic) Set(topic *Topic) error {
	v, err := EncodeGob(topic)
	if err != nil {
		return err
	}
	return d.store.Set(d.prefix(topic.Name), v)
}

func (d *DatastoreTopic) Delete(key string) error {
	return d.store.Delete(d.prefix(key))
}

func (d *DatastoreTopic) prefix(key string) string {
	return "topic_" + key
}
