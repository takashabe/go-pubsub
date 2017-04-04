package queue

import (
	"time"

	"github.com/pkg/errors"
)

type DatastoreMessage struct {
	store Datastore
}

func NewDatastoreMessage() *DatastoreMessage {
	// TODO: flexible datastore source
	return &DatastoreMessage{
		store: NewMemory(),
	}
}

func (m *DatastoreMessage) Get(key string) (*Message, error) {
	t := m.store.Get(key)
	if t == nil {
		return nil, errors.Wrapf(ErrNotFoundMessage, "key=%s", key)
	}
	v, ok := t.(*Message)
	if !ok {
		return nil, errors.Wrapf(ErrNotMatchTypeMessage, "key=%s", key)
	}
	return v, nil
}

func (m *DatastoreMessage) List() ([]*Message, error) {
	values := m.store.Dump()
	res := make([]*Message, 0, len(values))
	for k, v := range values {
		if vt, ok := v.(*Message); ok {
			res = append(res, vt)
		} else {
			return nil, errors.Wrapf(ErrNotMatchTypeMessage, "key=%s", k)
		}
	}
	return res, nil
}

func (m *DatastoreMessage) Set(v *Message) error {
	return m.store.Set(v.ID, v)
}

func (m *DatastoreMessage) Delete(key string) error {
	return m.store.Delete(key)
}

func (m *DatastoreMessage) Size() int {
	return len(m.store.Dump())
}

func (m *DatastoreMessage) FindByReadable(name string, timeout time.Duration, size int) ([]*Message, error) {
	switch m.store.(type) {
	case *Memory:
		source := m.store.Dump()
		dst := make([]*Message, 0)
		for k, v := range source {
			if len(dst) > size {
				return dst, nil
			}
			msg, ok := v.(*Message)
			if !ok {
				return nil, errors.Wrapf(ErrNotMatchTypeMessage, "key=%s", k)
			}
			if msg.Readable(name, timeout) {
				dst = append(dst, msg)
			}
		}
		return dst, nil
	// TODO: impl case *MySQL:
	default:
		return nil, ErrNotSupportOperation
	}
}
