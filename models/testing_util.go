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
		// load memory
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

	// flush datastore. only Redis
	d, err := LoadDatastore(globalConfig)
	if err != nil {
		t.Fatalf("failed to load datastore, got err %v", err)
	}
	if redis, ok := d.(*Redis); ok {
		if _, err := redis.conn.Do("FLUSHDB"); err != nil {
			t.Fatalf("failed to FLUSHDB on Redis, got error %v", err)
		}
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

func setupTopic(t *testing.T, name string) *Topic {
	topic, err := NewTopic(name)
	if err != nil {
		t.Fatalf("failed to create topic, key=%s", name)
	}
	return topic
}

func setupDummyTopics(t *testing.T) {
	dummies := []string{"A", "B", "C"}
	for _, a := range dummies {
		setupTopic(t, a)
	}
}

// setupSubscription requires Topic
func setupSubscription(t *testing.T, name, topicName string) *Subscription {
	s, err := NewSubscription(name, topicName, 10, "", nil)
	if err != nil {
		t.Fatalf("failed to cretae Subscription, got error %v", err)
	}
	return s
}

func setupDummySubscription(t *testing.T) {
	dummies := []string{"a", "b"}
	for _, a := range dummies {
		setupSubscription(t, a, "A")
	}
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
