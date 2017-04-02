package queue

import (
	"reflect"
	"testing"
	"time"
)

func TestModifyState(t *testing.T) {
	// Warning: not cleanup topic and subscriptions each testcase
	// States flow: wait > deliver > ack
	helper.setupGlobalAndSetTopics(t, "topA")
	TopA, err := GetTopic("topA")
	if err != nil {
		t.Fatalf(err.Error())
	}
	subA, _ := NewSubscription("subA", "topA", 100, "localhost", nil)
	subB, _ := NewSubscription("subB", "topA", 100, "localhost", nil)
	TopA.AddSubscription(subA)
	TopA.AddSubscription(subB)
	topaSubscriptions, err := TopA.GetSubscriptions()
	if err != nil {
		t.Fatalf("failed to get subscriptions from topic")
	}
	baseMsg := NewMessage("foo", *TopA, []byte("test"), nil, topaSubscriptions)

	cases := []struct {
		inputSubID   []string
		inputFn      func(msg *Message, id string)
		expectStates map[string]messageState
	}{
		{
			[]string{"subA"},
			func(msg *Message, id string) { msg.Deliver(id) },
			map[string]messageState{
				"subA": stateDeliver,
				"subB": stateWait,
			},
		},
		{
			[]string{"subA", "subB"},
			func(msg *Message, id string) { msg.Ack(id) },
			map[string]messageState{
				"subA": stateAck,
				"subB": stateWait,
			},
		},
	}
	for i, c := range cases {
		for _, id := range c.inputSubID {
			c.inputFn(baseMsg, id)
		}
		got := baseMsg.States.list
		if !reflect.DeepEqual(got, c.expectStates) {
			t.Errorf("#%d: want %v, got %v", i, c.expectStates, got)
		}
	}
}

func TestReadable(t *testing.T) {
	helper.setupGlobal()
	baseMsg := Message{
		States: &states{
			list: map[string]messageState{
				"A": stateWait,
				"B": stateDeliver,
				"C": stateAck,
			},
		},
	}

	cases := []struct {
		wait      time.Duration
		inputID   string
		inputTime time.Duration
		expect    bool
	}{
		{
			0,
			"A",
			10 * time.Millisecond,
			true,
		},
		{
			0,
			"B",
			10 * time.Millisecond,
			false,
		},
		{
			20 * time.Millisecond,
			"B",
			10 * time.Millisecond,
			true,
		},
		{
			20 * time.Millisecond,
			"C",
			10 * time.Millisecond,
			false,
		},
	}
	for i, c := range cases {
		msg := baseMsg
		msg.DeliveredAt = time.Now()
		time.Sleep(c.wait)
		got := msg.Readable(c.inputID, c.inputTime)
		if got != c.expect {
			t.Error("#%d: want %v, got %v", i, c.expect, got)
		}
	}
}
