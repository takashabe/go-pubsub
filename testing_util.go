package queue

type testHelper struct{}

var helper = testHelper{}

func (h *testHelper) setupGlobal() {
	GlobalTopics = newTopics()
}

func (h *testHelper) setupGlobalAndSetTopics(names ...string) {
	GlobalTopics = newTopics()
	for _, v := range names {
		GlobalTopics.Set(h.dummyTopic(v))
	}
}

func (h *testHelper) dummyTopic(name string) *Topic {
	return &Topic{
		name: name,
	}
}

func (h *testHelper) dummyTopics(args ...string) *topics {
	m := newTopics()
	for _, a := range args {
		m.Set(h.dummyTopic(a))
	}
	return m
}

type testDatastore struct{}

func newTestDatastore() *testDatastore {
	return &testDatastore{}
}

func (d *testDatastore) Set(m Message) error {
	return nil
}

func (d *testDatastore) Get(key string) (Message, error) {
	return Message{}, nil
}
