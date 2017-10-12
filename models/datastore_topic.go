package models

import (
	"bytes"
	"encoding/gob"

	"github.com/pkg/errors"
	"github.com/takashabe/go-pubsub/datastore"
)

// globalTopics global Topic datastore
var globalTopics *DatastoreTopic

// DatastoreTopic is adapter between actual datastore and datastore client
type DatastoreTopic struct {
	store datastore.Datastore
}

// NewDatastoreTopic create DatastoreTopic object
func NewDatastoreTopic(cfg *datastore.Config) (*DatastoreTopic, error) {
	d, err := datastore.LoadDatastore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load datastore")
	}
	return &DatastoreTopic{
		store: d,
	}, nil
}

// InitDatastoreTopic initialize global datastore object
func InitDatastoreTopic() error {
	d, err := NewDatastoreTopic(datastore.GlobalConfig)
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

// Get return item via datastore
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

// List return all topic slice
func (d *DatastoreTopic) List() ([]*Topic, error) {
	sources, err := datastore.SpecifyDump(d.store, d.prefix(""))
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

// Set save item to datastore
func (d *DatastoreTopic) Set(topic *Topic) error {
	v, err := datastore.EncodeGob(topic)
	if err != nil {
		return err
	}
	return d.store.Set(d.prefix(topic.Name), v)
}

// Delete delete item
func (d *DatastoreTopic) Delete(key string) error {
	return d.store.Delete(d.prefix(key))
}

func (d *DatastoreTopic) prefix(key string) string {
	return "topic_" + key
}
