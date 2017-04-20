package models

import "testing"

type testHelper struct {
	dummyConfig *Config
}

var helper = testHelper{
	dummyConfig: &Config{Driver: "memory"},
}

func (h *testHelper) setupGlobal(t *testing.T) {
	if d, err := NewDatastoreTopic(h.dummyConfig); err != nil {
		t.Fatal(err)
	} else {
		globalTopics = d
	}
	if d, err := NewDatastoreSubscription(h.dummyConfig); err != nil {
		t.Fatal(err)
	} else {
		globalSubscription = d
	}
	if d, err := NewDatastoreMessage(h.dummyConfig); err != nil {
		t.Fatal(err)
	} else {
		globalMessage = d
	}
	globalConfig = h.dummyConfig
}

func (h *testHelper) setupGlobalAndSetTopics(t *testing.T, names ...string) {
	h.setupGlobal(t)
	for _, v := range names {
		globalTopics.Set(h.dummyTopic(t, v))
	}
}

func (h *testHelper) dummyTopic(t *testing.T, name string) *Topic {
	return &Topic{
		Name: name,
	}
}

func (h *testHelper) dummyTopics(t *testing.T, args ...string) *DatastoreTopic {
	m, _ := NewDatastoreTopic(nil)
	for _, a := range args {
		m.Set(h.dummyTopic(t, a))
	}
	return m
}

func (h *testHelper) dummyMessage(t *testing.T, id string) *Message {
	return &Message{
		ID: id,
	}
}

func (h *testHelper) dummyMessageWithState(t *testing.T, id string, state map[string]messageState) *Message {
	return &Message{
		ID:            id,
		Subscriptions: NewMemory(nil),
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
