package models

import (
	"fmt"
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
	helper.setupGlobalAndSetTopics(t, "a")
	ms, err := newMessageStatusStore(nil)
	if err != nil {
		t.Fatalf("failed to create MessageStatusStore, got err %v", err)
	}
	expect1 := &Subscription{
		Name:               "A",
		TopicID:            "a",
		MessageStatus:      ms,
		DefaultAckDeadline: 0,
		Push: &Push{
			Endpoint: testUrl(t, "localhost:8080"),
			Attributes: &Attributes{
				Attr: map[string]string{"key": "value"},
			},
		},
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
			"A", "a", -1, "localhost:8080", map[string]string{"key": "value"},
			expect1,
			nil,
		},
		{ // same name Subscription
			"A", "a", -1, "localhost:8080", map[string]string{"key": "value"},
			nil,
			ErrAlreadyExistSubscription,
		},
		{
			"B", "b", -1, "localhost:8080", map[string]string{"key": "value"},
			nil,
			ErrNotFoundTopic,
		},
		{
			"B", "a", -1, ":", map[string]string{"key": "value"},
			nil,
			ErrInvalidEndpoint,
		},
	}
	for i, c := range cases {
		got, err := NewSubscription(c.name, c.topicName, c.timeout, c.endpoint, c.attr)
		if errors.Cause(err) != c.expectErr {
			t.Fatalf("%#d: want %v, got %v", i, c.expectErr, err)
		}
		if !reflect.DeepEqual(got, c.expectObj) {
			t.Errorf("%#d: want %#v, got %#v", i, c.expectObj, got)
		}
	}
}

func TestDeleteSubscription(t *testing.T) {
	helper.setupGlobal(t)
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
	helper.setupGlobalAndSetTopics(t, "a", "b")

	// make Subscription and Topic
	sub, err := NewSubscription("A", "a", 0, "", nil)
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	sub.DefaultAckDeadline = 100 * time.Millisecond // faster for test
	topics, err := ListTopic()
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	for _, topic := range topics {
		topic.Publish([]byte(fmt.Sprintf("%s-test", topic.Name)), nil)
	}
	// want only Topic "a"
	got, err := sub.MessageStatus.GetRangeMessage(2)
	if err != nil {
		t.Fatalf("failed get message. err=%v", err)
	}
	if want := []string{"a-test"}; !isExistMessageData(got, want) {
		t.Errorf("want exist %v in MessageList, got %v", want, got)
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
	aTopic, err := GetTopic("a")
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	aTopic.Publish([]byte("test"), nil)
	pullMsgs, err = sub.Pull(1)
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}

	func() {
		after1 := time.After(50 * time.Millisecond)
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
