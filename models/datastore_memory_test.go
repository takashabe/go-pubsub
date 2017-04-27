package models

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"
)

func TestLoadDatastore(t *testing.T) {
	cases := []struct {
		input  *Config
		expect string
	}{
		{
			&Config{Datastore: &DatastoreConfig{}},
			"*models.Memory",
		},
		{
			// WARNING: Require connect redis
			&Config{
				Datastore: &DatastoreConfig{
					Redis: &RedisConfig{
						Host: "localhost",
						Port: 6379,
						DB:   0,
					},
				}},
			"*models.Redis",
		},
	}
	for i, c := range cases {
		d, err := LoadDatastore(c.input)
		if err != nil {
			t.Fatalf("#%d: failed to load datastore, got err %v", i, err)
		}
		if got := reflect.TypeOf(d); got.String() != c.expect {
			t.Errorf("#%d: datastore type want %s, got %s", i, c.expect, got)
		}
	}
}

func TestMemorySet(t *testing.T) {
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
		m := NewMemory(nil)
		for _, v := range c.inputMsgs {
			m.Set(v.ID, v)
		}
		if got := m.Store; !reflect.DeepEqual(got, c.expect) {
			t.Errorf("#%d: want %v, got %v", i, c.expect, got)
		}
	}
}

func TestMemoryGet(t *testing.T) {
	msgA := Message{ID: "a"}
	msgB := Message{ID: "b"}
	baseStore := Memory{
		Store: map[interface{}]interface{}{"a": msgA, "b": msgB},
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
		got, err := baseStore.Get(c.input)
		if err != nil {
			t.Fatalf("#%d: failed to get item, got error %v", i, err)
		}
		if !reflect.DeepEqual(got, c.expectMsg) {
			t.Errorf("#%d: want %v, got %v", i, c.expectMsg, got)
		}
	}
}

func TestMemoryDelete(t *testing.T) {
	msgA := Message{ID: "a"}
	msgB := Message{ID: "b"}
	baseStore := Memory{
		Store: map[interface{}]interface{}{"a": msgA, "b": msgB},
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
		if !reflect.DeepEqual(baseStore.Store, c.expectStore) {
			t.Errorf("#%d: want %v, got %v", i, c.expectStore, baseStore.Store)
		}
	}
}
