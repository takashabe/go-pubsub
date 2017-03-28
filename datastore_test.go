package queue

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"
)

func TestSet(t *testing.T) {
	msgA := Message{ID: "a"}
	msgB := Message{ID: "b"}

	cases := []struct {
		inputMsgs []Message
		expect    map[string]Message
	}{
		{
			[]Message{
				msgA, msgA, msgB,
			},
			map[string]Message{
				"a": msgA, "b": msgB,
			},
		},
	}
	for i, c := range cases {
		m := NewMemory()
		for _, v := range c.inputMsgs {
			m.Set(v)
		}
		if got := m.messages; !reflect.DeepEqual(got, c.expect) {
			t.Errorf("#%d: want %v, got %v", i, c.expect, got)
		}
	}
}

func TestGet(t *testing.T) {
	msgA := Message{ID: "a"}
	msgB := Message{ID: "b"}
	baseStore := Memory{
		messages: map[string]Message{"a": msgA, "b": msgB},
	}

	cases := []struct {
		input     string
		expectMsg Message
		expectErr error
	}{
		{
			"a",
			msgA,
			nil,
		},
		{
			"c",
			Message{},
			ErrNotFoundMessage,
		},
	}
	for i, c := range cases {
		got, err := baseStore.Get(c.input)
		if errors.Cause(err) != c.expectErr {
			t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		}
		if !reflect.DeepEqual(got, c.expectMsg) {
			t.Errorf("#%d: want %v, got %v", i, c.expectMsg, got)
		}
	}
}
