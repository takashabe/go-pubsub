package models

import (
	"os"
	"testing"
	"time"

	fixture "github.com/takashabe/go-fixture"
	_ "github.com/takashabe/go-fixture/mysql"
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
		path = "testdata/config/memory.yaml"
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
	if err := InitDatastoreMessageStatus(); err != nil {
		t.Fatal(err)
	}

	// flush datastore
	d, err := LoadDatastore(globalConfig)
	if err != nil {
		t.Fatalf("failed to load datastore, got err %v", err)
	}
	switch a := d.(type) {
	case *Redis:
		conn := a.Pool.Get()
		defer conn.Close()

		_, err := conn.Do("FLUSHDB")
		if err != nil {
			t.Fatalf("failed to FLUSHDB on Redis, got error %v", err)
		}
	case *MySQL:
		f := fixture.NewFixture(a.Conn, "mysql")
		if err := f.LoadSQL("fixture/setup_mq_table.sql"); err != nil {
			t.Fatalf("failed to execute fixture, got err %v", err)
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

// publishMessage requires Topic
func publishMessage(t *testing.T, topicID, message string, attr map[string]string) string {
	top, err := GetTopic(topicID)
	if err != nil {
		t.Fatalf("failed to get topic, got error %v", err)
	}
	msgID, err := top.Publish([]byte(message), attr)
	if err != nil {
		t.Fatalf("failed to publish message, got error %v", err)
	}
	return msgID
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

func mustGetTopic(t *testing.T, id string) *Topic {
	a, err := GetTopic(id)
	if err != nil {
		t.Fatalf("failed to get topic, got err %v", err)
	}
	return a
}

func mustGetSubscription(t *testing.T, id string) *Subscription {
	a, err := GetSubscription(id)
	if err != nil {
		t.Fatalf("failed to get subscription, got err %v", err)
	}
	return a
}

func WaitPushMessaging(t *testing.T, reqCount *int, messageSize int) {
	failCount := 0
	for {
		if *reqCount >= messageSize {
			return
		}

		if failCount > 100 {
			t.Fatalf("failed to push message, timeout error")
		}
		failCount++
		time.Sleep(10 * time.Millisecond)
	}
}

func waitPushRunningDisable(t *testing.T, subID string) {
	failCount := 0
	for {
		s := mustGetSubscription(t, subID)
		if !s.getRunning() {
			return
		}
		if failCount >= 100 {
			t.Fatalf("failed to wait PushRunning disabled, timeout error")
		}
		failCount++
		time.Sleep(10 * time.Millisecond)
	}
}
