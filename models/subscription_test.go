package models

import (
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func testUrl(t *testing.T, raw string) *url.URL {
	url, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("failed to parse URL: %s", raw)
	}
	return url
}

func TestNewSubscription(t *testing.T) {
	setupDatastore(t)
	setupDummyTopics(t)

	expect1 := &Subscription{
		Name:               "A",
		TopicID:            "A",
		Message:            NewMessageStatusStore("A"),
		DefaultAckDeadline: 0,
		PushConfig: &Push{
			Endpoint: testUrl(t, "localhost:8080"),
			Attributes: &Attributes{
				Attr: map[string]string{"key": "value"},
			},
		},
		AbortPush:   nil,
		PushRunning: true,
		PushTick:    PushInterval,
		PushSize:    MinPushSize,
	}

	cases := []struct {
		name      string
		topicName string
		timeout   int64
		endpoint  string
		attr      map[string]string
		expectObj *Subscription
		expectErr error
	}{
		{
			"A", "A", -1, "localhost:8080", map[string]string{"key": "value"},
			expect1,
			nil,
		},
		{ // same name Subscription
			"A", "A", -1, "localhost:8080", map[string]string{"key": "value"},
			nil,
			ErrAlreadyExistSubscription,
		},
		{
			"B", "_", -1, "localhost:8080", map[string]string{"key": "value"},
			nil,
			ErrNotFoundEntry,
		},
		{
			"B", "A", -1, ":", map[string]string{"key": "value"},
			nil,
			ErrInvalidEndpoint,
		},
	}
	for i, c := range cases {
		got, err := NewSubscription(c.name, c.topicName, c.timeout, c.endpoint, c.attr)
		if errors.Cause(err) != c.expectErr {
			t.Fatalf("%#d: want %v, got %v", i, c.expectErr, err)
		}
		if got != nil {
			got.AbortPush = nil // for channel equal
		}
		if !reflect.DeepEqual(got, c.expectObj) {
			t.Errorf("%#d: want %#v, got %#v", i, c.expectObj, got)
		}
	}
}

func TestDeleteSubscription(t *testing.T) {
	setupDatastore(t)
	subA := &Subscription{Name: "A", TopicID: "a"}
	subB := &Subscription{Name: "B", TopicID: "a"}
	globalSubscription.Set(subA)
	globalSubscription.Set(subB)

	cases := []struct {
		input          *Subscription
		expectSubNames []string
	}{
		{subA, []string{"B"}},
		{subA, []string{"B"}}, // already deleted
		{subB, []string{}},
	}
	for i, c := range cases {
		err := c.input.Delete()
		if err != nil {
			t.Errorf("#%d: want no error, got %v", i, err)
		}
		names := []string{}
		if list, err := ListSubscription(); err != nil {
			t.Fatalf("#%d: want no error, got %v", i, err)
		} else {
			for _, s := range list {
				names = append(names, s.Name)
			}
		}
		if !reflect.DeepEqual(names, c.expectSubNames) {
			t.Errorf("#%d: want %v, got %v", i, c.expectSubNames, names)
		}
	}
}

func TestPullAndAck(t *testing.T) {
	setupDatastore(t)
	setupDummyTopics(t)
	setupDummySubscription(t)

	// faster for test
	if s, err := GetSubscription("a"); err != nil {
		t.Fatalf("failed to get Subscription, got error %v", err)
	} else {
		s.DefaultAckDeadline = 100 * time.Millisecond
		if err = s.Save(); err != nil {
			t.Fatalf("failed to save Subscription, got error %v", err)
		}
	}

	// publish message
	msgID := publishMessage(t, "A", "test", nil)

	// collect a message
	sub, err := GetSubscription("a") // get updated Subscription
	if err != nil {
		t.Fatalf("failed to get Subscription, got error %v", err)
	}
	got, err := sub.Message.CollectReadableMessage(2)
	if err != nil {
		t.Fatalf("failed to collect message. err=%v", err)
	}
	if got[0].ID != msgID {
		t.Errorf("want message ID %s, got %s", msgID, got[0].ID)
	}

	// pull and ack
	pullMsgs, err := sub.Pull(1)
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	for _, m := range pullMsgs {
		if err := sub.Ack(m.AckID); err != nil {
			t.Fatalf("failed to ack, got err %v", err)
		}
	}
	if want := 1; len(pullMsgs) != want {
		t.Errorf("want len %d, got len %d", want, len(pullMsgs))
	}
	// pull from empty
	_, err = sub.Pull(1)
	if err != ErrEmptyMessage {
		t.Errorf("want %v, got %v", ErrEmptyMessage, err)
	}

	// pull and none send ack, retry pull
	publishMessage(t, "A", "test", nil)
	sub, err = GetSubscription("a") // get updated Subscription
	if err != nil {
		t.Fatalf("failed to get Subscription, got error %v", err)
	}
	_, err = sub.Pull(1)
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}

	func() {
		after1 := time.After(10 * time.Millisecond)
		after2 := time.After(100 * time.Millisecond)
		for {
			select {
			case <-after1:
				// before ack timeout
				_, err = sub.Pull(1)
				if want := ErrEmptyMessage; err != want {
					t.Errorf("want %v, got %v", want, err)
				}
			case <-after2:
				// after ack timeout
				pullMsgs, err = sub.Pull(1)
				if err != nil {
					t.Errorf("want no error, got %v", err)
				}
				if want := 1; len(pullMsgs) != want {
					t.Errorf("want len %d, got len %d", want, len(pullMsgs))
				}
				return
			}
		}
	}()
}

func TestPushImmediately(t *testing.T) {
	setupDatastore(t)
	setupDummyTopics(t)
	setupDummySubscription(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer ts.Close()

	err := mustGetSubscription(t, "a").SetPushConfig(ts.URL, nil)
	if err != nil {
		t.Fatalf("failed to SetPushConfig, got err %v", err)
	}

	msgID, err := mustGetTopic(t, "A").Publish([]byte("test"), map[string]string{"1": "2"})
	if err != nil {
		t.Fatalf("failed to Publish, got err %v", err)
	}

	// not exist message when push and ack message
	_, err = globalMessageStatus.FindBySubscriptionIDAndMessageID("a", msgID)
	if err != ErrNotFoundEntry {
		t.Errorf("error want %s , got %s", ErrNotFoundEntry, err)
	}
}

func TestPushLoop(t *testing.T) {
	setupDatastore(t)
	setupDummyTopics(t)
	setupDummySubscription(t)

	var wg sync.WaitGroup
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		w.WriteHeader(200)
	}))
	defer ts.Close()

	// publish message at pull mode
	for i := 0; i < 3; i++ {
		wg.Add(1)
		publishMessage(t, "A", "test", nil)
	}

	// set to push mode
	sub := mustGetSubscription(t, "a")
	sub.PushTick = 10 * time.Millisecond // faster testing
	if err := sub.SetPushConfig(ts.URL, nil); err != nil {
		t.Fatalf("failed to SetPushConfig, got err %v", err)
	}

	// wait push messaging
	wg.Wait()
	if err := mustGetSubscription(t, "a").SetPushConfig("", nil); err != nil {
		t.Fatalf("failed to SetPushConfig, got err %v", err)
	}
	waitPushRunningDisable(t, "a")

	// want empty message
	list, err := globalMessageStatus.collectByField(func(ms *MessageStatus) bool {
		return ms.SubscriptionID == sub.Name
	})
	if err != nil {
		t.Fatalf("failed to collect MessageStatus, got err %v", err)
	}
	if len(list) != 0 {
		t.Errorf("MessageStatus list size want 0, got %d", len(list))
	}
}

func TestPushLoopIncrement(t *testing.T) {
	setupDatastore(t)
	setupDummyTopics(t)
	setupDummySubscription(t)

	var wg sync.WaitGroup
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		w.WriteHeader(200)
	}))
	defer ts.Close()

	// publish message at pull mode
	for i := 0; i < 3; i++ {
		wg.Add(1)
		publishMessage(t, "A", "test", nil)
	}

	// set to push mode
	sub := mustGetSubscription(t, "a")
	sub.PushTick = 10 * time.Millisecond  // faster testing
	sub.PushSize = int(MaxPushSize/2) + 1 // want max size at next loop
	if err := sub.SetPushConfig(ts.URL, nil); err != nil {
		t.Fatalf("failed to SetPushConfig, got err %v", err)
	}

	// wait push messaging
	wg.Wait()
	if err := mustGetSubscription(t, "a").SetPushConfig("", nil); err != nil {
		t.Fatalf("failed to SetPushConfig, got err %v", err)
	}
	waitPushRunningDisable(t, "a")

	// want empty message
	list, err := globalMessageStatus.collectByField(func(ms *MessageStatus) bool {
		return ms.SubscriptionID == sub.Name
	})
	if err != nil {
		t.Fatalf("failed to collect MessageStatus, got err %v", err)
	}
	if len(list) != 0 {
		t.Errorf("MessageStatus list size want 0, got %d", len(list))
	}

	// want MaxPushSize
	if got := mustGetSubscription(t, "a").PushSize; got != MaxPushSize {
		t.Errorf("want push size = %d, got %d", MaxPushSize, got)
	}
}

func TestPushLoopDecrement(t *testing.T) {
	setupDatastore(t)
	setupDummyTopics(t)
	setupDummySubscription(t)

	var wg sync.WaitGroup
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		w.WriteHeader(500)
	}))
	defer ts.Close()

	// publish message at pull mode
	messageSize := 3
	for i := 0; i < messageSize; i++ {
		wg.Add(1)
		publishMessage(t, "A", "test", nil)
	}

	// set to push mode
	sub := mustGetSubscription(t, "a")
	sub.PushTick = 10 * time.Millisecond // faster testing
	sub.PushSize = MinPushSize
	if err := sub.SetPushConfig(ts.URL, nil); err != nil {
		t.Fatalf("failed to SetPushConfig, got err %v", err)
	}

	// wait push messaging
	wg.Wait()
	if err := mustGetSubscription(t, "a").SetPushConfig("", nil); err != nil {
		t.Fatalf("failed to SetPushConfig, got err %v", err)
	}
	waitPushRunningDisable(t, "a")

	// want fullsize message
	list, err := globalMessageStatus.collectByField(func(ms *MessageStatus) bool {
		return ms.SubscriptionID == sub.Name
	})
	if err != nil {
		t.Fatalf("failed to collect MessageStatus, got err %v", err)
	}
	if len(list) != messageSize {
		t.Errorf("MessageStatus list size want %d, got %d", messageSize, len(list))
	}

	// want MinPushSize
	if got := mustGetSubscription(t, "a").PushSize; got != MinPushSize {
		t.Errorf("want push size = %d, got %d", MinPushSize, got)
	}
}
