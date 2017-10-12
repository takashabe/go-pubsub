package models

import (
	"bytes"
	"encoding/gob"

	"github.com/pkg/errors"
	"github.com/takashabe/go-pubsub/datastore"
)

// globalMessage Global message key-value store object
var globalMessage *DatastoreMessage

// DatastoreMessage is adapter between actual datastore and datastore client
type DatastoreMessage struct {
	store datastore.Datastore
}

// NewDatastoreMessage create DatastoreTopic object
func NewDatastoreMessage(cfg *datastore.Config) (*DatastoreMessage, error) {
	d, err := datastore.LoadDatastore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load datastore")
	}
	return &DatastoreMessage{
		store: d,
	}, nil
}

// InitDatastoreMessage initialize global datastore object
func InitDatastoreMessage() error {
	d, err := NewDatastoreMessage(datastore.GlobalConfig)
	if err != nil {
		return err
	}
	globalMessage = d
	return nil
}

// decodeRawMessage return Message from encode raw data
func decodeRawMessage(r interface{}) (*Message, error) {
	switch a := r.(type) {
	case []byte:
		return decodeGobMessage(a)
	default:
		return nil, ErrNotMatchTypeMessage
	}
}

// decodeGobMessage return Message from gob encode data
func decodeGobMessage(e []byte) (*Message, error) {
	var res *Message
	buf := bytes.NewReader(e)
	if err := gob.NewDecoder(buf).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

// Get return item via datastore
func (d *DatastoreMessage) Get(key string) (*Message, error) {
	v, err := d.store.Get(d.prefix(key))
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrNotFoundEntry
	}
	return decodeRawMessage(v)
}

// Set save item to datastore
func (d *DatastoreMessage) Set(m *Message) error {
	v, err := datastore.EncodeGob(m)
	if err != nil {
		return err
	}
	return d.store.Set(d.prefix(m.ID), v)
}

// Delete delete item
func (d *DatastoreMessage) Delete(key string) error {
	return d.store.Delete(d.prefix(key))
}

func (d *DatastoreMessage) prefix(key string) string {
	return "message_" + key
}
