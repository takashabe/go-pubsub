package queue

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

	expect1 := &Subscription{
		name:       "A",
		topic:      helper.dummyTopic(t, "a"),
		messages:   newMessageList(),
		ackTimeout: 0,
		push: &Push{
			endpoint: testUrl(t, "localhost:8080"),
			attributes: &Attributes{
				attr: map[string]string{"key": "value"},
			},
		},
	}
	expect1.topic.AddSubscription(expect1)

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
		{
			"A", "b", -1, "localhost:8080", map[string]string{"key": "value"},
			nil,
			ErrNotFoundTopic,
		},
		{
			"A", "a", -1, ":", map[string]string{"key": "value"},
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

func TestGetRange(t *testing.T) {
	// Warning: not clean message list each testcases
	msgWait := helper.dummyMessageWithState(t, "wait", map[string]messageState{"A": stateWait})
	msgDeliver1 := helper.dummyMessageWithState(t, "deliver1", map[string]messageState{"A": stateDeliver})
	msgDeliver2 := helper.dummyMessageWithState(t, "deliver2", map[string]messageState{"A": stateDeliver})
	msgAck := helper.dummyMessageWithState(t, "ack", map[string]messageState{"A": stateAck})
	baseMsgList := MessageList{
		list: []*Message{
			msgWait, msgDeliver1, msgDeliver2, msgAck,
		},
	}

	cases := []struct {
		inputSub  *Subscription
		inputSize int
		wait      time.Duration
		addTime   time.Duration
		expectMsg []*Message
		expectErr error
	}{
		{
			&Subscription{
				name:       "A",
				messages:   &baseMsgList,
				ackTimeout: 0 * time.Millisecond,
			},
			4,
			0 * time.Millisecond,
			0 * time.Millisecond,
			[]*Message{msgWait, msgDeliver1, msgDeliver2},
			nil,
		},
		{
			// timeout
			&Subscription{
				name:       "A",
				messages:   &baseMsgList,
				ackTimeout: 1000 * time.Millisecond,
			},
			2,
			0 * time.Millisecond,
			0 * time.Millisecond,
			[]*Message{msgWait},
			nil,
		},
		{
			&Subscription{
				name:       "A",
				messages:   &baseMsgList,
				ackTimeout: 0 * time.Millisecond,
			},
			2,
			0,
			1000 * time.Millisecond, // deliver at future
			[]*Message{msgWait, msgDeliver1},
			nil,
		},
	}
	for i, c := range cases {
		// msgDeliver set DeliveredAt
		msgDeliver1.DeliveredAt = time.Now()
		msgDeliver2.DeliveredAt = time.Now().Add(c.addTime)

		time.Sleep(c.wait)
		got, err := baseMsgList.GetRange(c.inputSub, c.inputSize)
		if err != c.expectErr {
			t.Errorf("%#d: want %v, got %v", i, c.expectErr, err)
		}
		if !reflect.DeepEqual(got, c.expectMsg) {
			t.Errorf("%#d: want %v, got %#v", i, c.expectMsg, got)
		}
	}
}

func TestGetRangeWithAck(t *testing.T) {
	msgWait := helper.dummyMessageWithState(t, "wait", map[string]messageState{"A": stateWait})
	msgDeliver1 := helper.dummyMessageWithState(t, "deliver1", map[string]messageState{"A": stateDeliver})
	msgDeliver2 := helper.dummyMessageWithState(t, "deliver2", map[string]messageState{"A": stateDeliver})
	msgAck := helper.dummyMessageWithState(t, "ack", map[string]messageState{"A": stateAck})
	baseMsgList := MessageList{
		list: []*Message{
			msgWait, msgDeliver1, msgDeliver2, msgAck,
		},
	}
	msgDeliver1.DeliveredAt = time.Now()
	msgDeliver2.DeliveredAt = time.Now()

	// all get
	sub := &Subscription{
		name:       "A",
		messages:   &baseMsgList,
		ackTimeout: 0 * time.Millisecond,
	}
	got, err := baseMsgList.GetRange(sub, len(baseMsgList.list))
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	if want := []*Message{msgWait, msgDeliver1, msgDeliver2}; !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %#v", want, got)
	}

	// some ack
	msgDeliver1.Ack(sub.name)
	got, err = baseMsgList.GetRange(sub, len(baseMsgList.list))
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	if want := []*Message{msgWait, msgDeliver2}; !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %#v", want, got)
	}
}

func TestPullAndAck(t *testing.T) {
	helper.setupGlobalAndSetTopics(t, "a", "b")

	// make Subscription and Topic
	sub, err := NewSubscription("A", "a", 100, "", nil)
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	topics, err := ListTopic()
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	for _, topic := range topics {
		topic.Publish([]byte(fmt.Sprintf("%s-test", topic.name)), nil)
	}
	// want only Topic "a"
	if want := []string{"a-test"}; !isExistMessageData(sub.messages.list, want) {
		t.Errorf("want exist %v in MessageList, got %v", want, sub.messages.list)
	}

	// pull and ack
	message, err := sub.Pull(1)
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	for _, m := range message {
		sub.Ack(m.ID)
	}
	if want := 1; len(message) != want {
		t.Errorf("want len %d, got len %d", want, len(message))
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
	message, err = sub.Pull(1)
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
				message, err = sub.Pull(1)
				if err != nil {
					t.Errorf("want no error, got %v", err)
				}
				if want := 1; len(message) != want {
					t.Errorf("want len %d, got len %d", want, len(message))
				}
				return
			}
		}
	}()
}
