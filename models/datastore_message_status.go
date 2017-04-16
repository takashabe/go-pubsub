package models

import "github.com/pkg/errors"

// DatastoreMessageStatus is adapter between actual datastore and datastore client
type DatastoreMessageStatus struct {
	store Datastore
}

// NewDatastoreMessageStatus create DatastoreTopic object
func NewDatastoreMessageStatus(cfg *Config) (*DatastoreMessageStatus, error) {
	d, err := LoadDatastore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load datastore")
	}
	return &DatastoreMessageStatus{
		store: d,
	}, nil
}

// FindByMessageID return MessageStatus matched MessageID
func (m *DatastoreMessageStatus) FindByMessageID(key string) (*MessageStatus, error) {
	t := m.store.Get(key)
	if t == nil {
		return nil, errors.Wrapf(ErrNotFoundMessageStatus, "key=%s", key)
	}
	v, ok := t.(*MessageStatus)
	if !ok {
		return nil, errors.Wrapf(ErrNotMatchTypeMessageStatus, "key=%s", key)
	}
	return v, nil
}

// FindByAckID return MessageStatus matched AckID
func (m *DatastoreMessageStatus) FindByAckID(id string) (*MessageStatus, error) {
	switch m.store.(type) {
	case *Memory:
		source := m.store.Dump()
		for k, v := range source {
			ms, ok := v.(*MessageStatus)
			if !ok {
				return nil, errors.Wrapf(ErrNotMatchTypeMessage, "key=%s", k)
			}
			if ms.AckID == id {
				return ms, nil
			}
		}
		return nil, ErrNotFoundMessageStatus
	// TODO: impl case *MySQL:
	default:
		return nil, ErrNotSupportOperation
	}
}

// List return all MessageStatus slice
func (m *DatastoreMessageStatus) List() ([]*MessageStatus, error) {
	values := m.store.Dump()
	res := make([]*MessageStatus, 0, len(values))
	for k, v := range values {
		if vt, ok := v.(*MessageStatus); ok {
			res = append(res, vt)
		} else {
			return nil, errors.Wrapf(ErrNotMatchTypeMessageStatus, "key=%s", k)
		}
	}
	return res, nil
}

func (m *DatastoreMessageStatus) Set(v *MessageStatus) error {
	return m.store.Set(v.MessageID, v)
}

func (m *DatastoreMessageStatus) Delete(key string) error {
	return m.store.Delete(key)
}

func (m *DatastoreMessageStatus) Size() int {
	return len(m.store.Dump())
}

// CollectByReadable return MessageStatus slice matched readable message
func (m *DatastoreMessageStatus) CollectByReadableMessage(size int) ([]*Message, error) {
	switch m.store.(type) {
	case *Memory:
		source := m.store.Dump()
		dst := make([]*Message, 0)
		for k, v := range source {
			if len(dst) >= size {
				return dst, nil
			}
			ms, ok := v.(*MessageStatus)
			if !ok {
				return nil, errors.Wrapf(ErrNotMatchTypeMessageStatus, "key=%s", k)
			}
			if !ms.Readable() {
				continue
			}
			if msg, err := globalMessage.Get(ms.MessageID); err != nil {
				dst = append(dst, msg)
			}
		}
		return dst, nil
	// TODO: impl case *MySQL:
	default:
		return nil, ErrNotSupportOperation
	}
}
