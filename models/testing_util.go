package models

import (
	"os"
	"testing"
)

type testHelper struct {
	dummyConfig *Config
}

func setupDatastore(t *testing.T) {
	// load config
	var path string
	if env := os.Getenv("GO_MESSAGE_QUEUE_CONFIG"); len(env) != 0 {
		path = env
	} else {
		path = "testdata/none.yaml"
	}
	cfg, err := LoadConfigFromFile(path)
	if err != nil {
		t.Fatalf("failed to load config, got err %v", err)
	}
	globalConfig = cfg

	// setup global variables
	if err := InitDatastoreTopic(); err != nil {
		t.Fatal(err)
	}
	if err := InitDatastoreSubscription(); err != nil {
		t.Fatal(err)
	}
	if err := InitDatastoreMessage(); err != nil {
		t.Fatal(err)
	}
}

func setupDatastoreAndSetTopics(t *testing.T, names ...string) {
	setupDatastore(t)
	for _, v := range names {
		if _, err := NewTopic(v); err != nil {
			t.Fatalf("failed to new topic, got err %v", err)
		}
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
