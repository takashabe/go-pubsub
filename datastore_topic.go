package queue

type DatastoreTopic struct {
	store Datastore
}

func NewDatastoreTopic() *DatastoreTopic {
	// TODO: flexible datastore source
	return &DatastoreTopic{
		store: NewMemory(),
	}
}

func (ts *DatastoreTopic) Get(key string) (*Topic, bool) {
	t := ts.store.Get(key)
	if v, ok := t.(*Topic); ok {
		return v, true
	}
	return nil, false
}

func (ts *DatastoreTopic) List() ([]*Topic, error) {
	values := ts.store.Dump()
	res := make([]*Topic, 0, len(values))
	for _, v := range values {
		if vt, ok := v.(*Topic); ok {
			res = append(res, vt)
		} else {
			return nil, ErrInvalidTopic
		}
	}
	return res, nil
}

func (ts *DatastoreTopic) Set(topic *Topic) error {
	return ts.store.Set(topic.name, topic)
}

func (ts *DatastoreTopic) Delete(key string) error {
	return ts.store.Delete(key)
}
