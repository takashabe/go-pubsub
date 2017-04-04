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
		expect    map[interface{}]interface{}
	}{
		{
			[]Message{
				msgA, msgA, msgB,
			},
			map[interface{}]interface{}{
				"a": msgA, "b": msgB,
			},
		},
	}
	for i, c := range cases {
		m := NewMemory()
		for _, v := range c.inputMsgs {
			m.Set(v.ID, v)
		}
		if got := m.store; !reflect.DeepEqual(got, c.expect) {
			t.Errorf("#%d: want %v, got %v", i, c.expect, got)
		}
	}
}

func TestGet(t *testing.T) {
	msgA := Message{ID: "a"}
	msgB := Message{ID: "b"}
	baseStore := Memory{
		store: map[interface{}]interface{}{"a": msgA, "b": msgB},
	}

	cases := []struct {
		input     string
		expectMsg interface{}
		expectErr error
	}{
		{
			"a",
			msgA,
			nil,
		},
		{
			"c",
			nil,
			ErrNotFoundMessage,
		},
	}
	for i, c := range cases {
		got := baseStore.Get(c.input)
		if !reflect.DeepEqual(got, c.expectMsg) {
			t.Errorf("#%d: want %v, got %v", i, c.expectMsg, got)
		}
	}
}

func TestDeleteStore(t *testing.T) {
	msgA := Message{ID: "a"}
	msgB := Message{ID: "b"}
	baseStore := Memory{
		store: map[interface{}]interface{}{"a": msgA, "b": msgB},
	}

	cases := []struct {
		input       string
		expectStore map[interface{}]interface{}
		expectErr   error
	}{
		{
			"a",
			map[interface{}]interface{}{"b": msgB},
			nil,
		},
		{
			// delete to non exist key
			"a",
			map[interface{}]interface{}{"b": msgB},
			nil,
		},
	}
	for i, c := range cases {
		err := baseStore.Delete(c.input)
		if errors.Cause(err) != c.expectErr {
			t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		}
		if !reflect.DeepEqual(baseStore.store, c.expectStore) {
			t.Errorf("#%d: want %v, got %v", i, c.expectStore, baseStore.store)
		}
	}
}
