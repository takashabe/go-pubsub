package queue

import "testing"

type testHelper struct{}

var helper = testHelper{}

func (h *testHelper) setupGlobal() {
	GlobalTopics = newTopics()
}

func (h *testHelper) setupGlobalAndSetTopics(t *testing.T, names ...string) {
	GlobalTopics = newTopics()
	for _, v := range names {
		GlobalTopics.Set(h.dummyTopic(t, v))
	}
}

func (h *testHelper) dummyTopic(t *testing.T, name string) *Topic {
	return &Topic{
		name:          name,
		subscriptions: make(map[string]Subscription),
		store:         newTestDatastore(),
	}
}

func (h *testHelper) dummyTopics(t *testing.T, args ...string) *topics {
	m := newTopics()
	for _, a := range args {
		m.Set(h.dummyTopic(t, a))
	}
	return m
}

func (h *testHelper) dummyAcks(t *testing.T, ids ...string) *states {
	a := &states{
		list: make(map[string]messageState),
	}
	for _, id := range ids {
		a.add(id)
	}
	return a
}

func (h *testHelper) dummyMessageList(t *testing.T, ms ...*Message) *MessageList {
	list := &MessageList{
		list: make([]*Message, 0),
	}
	for _, m := range ms {
		list.Append(m)
	}
	return list
}

func (h *testHelper) dummyMessage(t *testing.T, id string) *Message {
	return &Message{
		ID: id,
	}
}

func isExistMessageID(src []*Message, subID []string) bool {
	srcMap := make(map[string]bool)
	for _, m := range src {
		srcMap[m.ID] = true
	}

	for _, id := range subID {
		if _, ok := srcMap[id]; !ok {
			// not found ID
			return false
		}
	}
	return true
}

func isExistMessageData(src []*Message, datas []string) bool {
	srcMap := make(map[string]bool)
	for _, m := range src {
		srcMap[string(m.Data)] = true
	}

	for _, d := range datas {
		if _, ok := srcMap[d]; !ok {
			// not found data
			return false
		}
	}
	return true
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
