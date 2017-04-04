package models

import (
	"fmt"
	"net/url"
	"reflect"
	"sort"
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
		{ // same name Subscription
			"A", "a", -1, "localhost:8080", map[string]string{"key": "value"},
			nil,
			ErrAlreadyExistSubscription,
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

func TestDeleteSubscription(t *testing.T) {
	helper.setupGlobal()
	suba := &Subscription{name: "A"}
	subb := &Subscription{name: "B"}
	globalSubscription.Set(suba)
	globalSubscription.Set(subb)

	cases := []struct {
		input          *Subscription
		expectSubNames []string
	}{
		{suba, []string{"B"}},
		{suba, []string{"B"}}, // already deleted
		{subb, []string{}},
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
				names = append(names, s.name)
			}
		}
		if !reflect.DeepEqual(names, c.expectSubNames) {
			t.Errorf("#%d: want %v, got %v", i, c.expectSubNames, names)
		}
	}
}

func TestGetRange(t *testing.T) {
	// Warning: not clean message list each testcases
	helper.setupGlobal()
	msgArgs := []struct {
		id    string
		staet messageState
	}{
		{"wait", stateWait},
		{"deliver1", stateDeliver},
		{"deliver2", stateDeliver},
		{"ack", stateAck},
	}
	baseMsg := make(map[string]*Message)
	for i, a := range msgArgs {
		baseMsg[a.id] = helper.dummyMessageWithState(t, a.id, map[string]messageState{"A": a.staet})
		if err := baseMsg[a.id].Save(); err != nil {
			t.Fatalf("#%d: failed msg save: err=%v", i, err)
		}
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
				messages:   newMessageList(),
				ackTimeout: 0 * time.Millisecond,
			},
			4,
			0 * time.Millisecond,
			0 * time.Millisecond,
			[]*Message{baseMsg["wait"], baseMsg["deliver1"], baseMsg["deliver2"]},
			nil,
		},
		{
			&Subscription{
				name:       "A",
				messages:   newMessageList(),
				ackTimeout: 1000 * time.Millisecond, // timeout
			},
			2,
			0 * time.Millisecond,
			0 * time.Millisecond,
			[]*Message{baseMsg["wait"]},
			nil,
		},
		{
			&Subscription{
				name:       "A",
				messages:   newMessageList(),
				ackTimeout: 0 * time.Millisecond,
			},
			2,
			0,
			1000 * time.Millisecond, // deliver at future
			[]*Message{baseMsg["wait"], baseMsg["deliver1"]},
			nil,
		},
	}
	for i, c := range cases {
		// msgDeliver set DeliveredAt
		baseMsg["deliver1"].DeliveredAt = time.Now()
		baseMsg["deliver2"].DeliveredAt = time.Now().Add(c.addTime)

		time.Sleep(c.wait)
		got, err := c.inputSub.messages.GetRange(c.inputSub, c.inputSize)
		if err != c.expectErr {
			t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		}
		want := c.expectMsg
		sort.Sort(ByMessageID(want))
		if !reflect.DeepEqual(got, want) {
			t.Errorf("#%d: want %v, got %#v", i, want, got)
		}
	}
}

func TestGetRangeWithAck(t *testing.T) {
	helper.setupGlobal()
	msgArgs := []struct {
		id    string
		staet messageState
	}{
		{"wait", stateWait},
		{"deliver1", stateDeliver},
		{"deliver2", stateDeliver},
		{"ack", stateAck},
	}
	baseMsg := make(map[string]*Message)
	for i, a := range msgArgs {
		baseMsg[a.id] = helper.dummyMessageWithState(t, a.id, map[string]messageState{"A": a.staet})
		if err := baseMsg[a.id].Save(); err != nil {
			t.Fatalf("#%d: failed msg save: err=%v", i, err)
		}
	}
	baseMsg["deliver1"].DeliveredAt = time.Now()
	baseMsg["deliver2"].DeliveredAt = time.Now()

	// all get
	sub := &Subscription{
		name:       "A",
		messages:   newMessageList(),
		ackTimeout: 0 * time.Millisecond,
	}
	got, err := sub.messages.GetRange(sub, sub.messages.list.Size())
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	want := []*Message{baseMsg["wait"], baseMsg["deliver1"], baseMsg["deliver2"]}
	sort.Sort(ByMessageID(want))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want %v, got %#v", want, got)
	}

	// some ack
	baseMsg["deliver1"].Ack(sub.name)
	got, err = sub.messages.GetRange(sub, sub.messages.list.Size())
	if err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	want = []*Message{baseMsg["wait"], baseMsg["deliver2"]}
	sort.Sort(ByMessageID(want))
	if !reflect.DeepEqual(got, want) {
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
	if got, err := sub.messages.GetRange(sub, 2); err == nil {
		if want := []string{"a-test"}; !isExistMessageData(got, want) {
			t.Errorf("want exist %v in MessageList, got %v", want, got)
		}
	} else {
		t.Fatalf("failed get message. err=%v", err)
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
