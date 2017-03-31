package queue

import (
	"reflect"
	"testing"
)

func TestNewTopic(t *testing.T) {
	cases := []struct {
		inputs            []string
		expectErr         error
		expectExistTopics []string
	}{
		{
			[]string{"a", "b"},
			nil,
			[]string{"a", "b"},
		},
		{
			[]string{"a", "a"},
			ErrAlreadyExistTopic,
			[]string{"a"},
		},
	}
	for i, c := range cases {
		helper.setupGlobal()
		var err error
		for _, s := range c.inputs {
			// expect last input return value equal expectErr
			_, err = NewTopic(s)
		}
		if err != c.expectErr {
			t.Errorf("%#d: want %v, got %v", i, c.expectErr, err)
		}

		list, err := GlobalTopics.List()
		if err != nil {
			t.Fatalf("%#d: want no error, got %v", i, err)
		}
		if len(list) != len(c.expectExistTopics) {
			t.Fatalf("%#d: want %d, got %d", len(c.expectExistTopics), len(list))
		}
		for i2, s := range c.expectExistTopics {
			if _, ok := GlobalTopics.Get(s); !ok {
				t.Errorf("#%d-%d: want exist Topic %s, but not exist", i, i2, s)
			}
		}
	}
}

func TestGetTopic(t *testing.T) {
	// make test topics
	helper.setupGlobal()
	GlobalTopics.Set(helper.dummyTopic(t, "a"))
	GlobalTopics.Set(helper.dummyTopic(t, "b"))

	cases := []struct {
		input       string
		expectTopic *Topic
		expectErr   error
	}{
		{"a", helper.dummyTopic(t, "a"), nil},
		{"c", nil, ErrNotFoundTopic},
	}
	for i, c := range cases {
		got, err := GetTopic(c.input)
		if err != c.expectErr {
			t.Errorf("%#d: want %v, got %v", i, c.expectErr, err)
		}
		if !reflect.DeepEqual(got, c.expectTopic) {
			t.Errorf("%#d: want %v, got %v", i, c.expectTopic, got)
		}
	}
}

func TestDelete(t *testing.T) {
	cases := []struct {
		baseTopics *topics
		input      *Topic
		expect     *topics
	}{
		{
			helper.dummyTopics(t, "a", "b"),
			helper.dummyTopic(t, "a"),
			helper.dummyTopics(t, "b"),
		},
		{
			helper.dummyTopics(t, "a", "b"),
			helper.dummyTopic(t, "c"),
			helper.dummyTopics(t, "a", "b"),
		},
	}
	for i, c := range cases {
		GlobalTopics = c.baseTopics
		// delete depends topic.name
		c.input.Delete()
		if !reflect.DeepEqual(GlobalTopics, c.expect) {
			t.Errorf("%#d: want %v, got %v", i, c.expect, GlobalTopics)
		}
	}
}

// TODO: integration datastore and subscription
func TestPublish(t *testing.T) {
	cases := []struct {
		inputData []byte
		inputAttr map[string]string
		expectErr error
	}{
		{
			[]byte(""), nil, nil,
		},
	}
	for i, c := range cases {
		topic := helper.dummyTopic(t, "a")
		got := topic.Publish(c.inputData, c.inputAttr)
		if got != c.expectErr {
			t.Errorf("%#d: want %v, got %v", i, c.expectErr, got)
		}
	}
}
