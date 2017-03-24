package queue

import (
	"reflect"
	"testing"
)

func TestNewTopic(t *testing.T) {
	cases := []struct {
		inputs       []string
		expectErr    error
		expectTopics *topics
	}{
		{
			[]string{"a", "b"},
			nil,
			helper.dummyTopics("a", "b"),
		},
		{
			[]string{"a", "a"},
			ErrAlreadyExistTopic,
			helper.dummyTopics("a"),
		},
	}
	for i, c := range cases {
		helper.setupGlobal()
		var err error
		for _, s := range c.inputs {
			// expect last input return value equal expectErr
			_, err = NewTopic(s, nil)
		}
		if err != c.expectErr {
			t.Errorf("%#d: want %v, got %v", i, c.expectErr, err)
		}
		if !reflect.DeepEqual(GlobalTopics, c.expectTopics) {
			t.Errorf("%#d: want %v, got %v", i, c.expectTopics, GlobalTopics)
		}
	}
}

func TestGetTopic(t *testing.T) {
	// make test topics
	helper.setupGlobal()
	GlobalTopics.Set(helper.dummyTopic("a"))
	GlobalTopics.Set(helper.dummyTopic("b"))

	cases := []struct {
		input       string
		expectTopic *Topic
		expectErr   error
	}{
		{"a", helper.dummyTopic("a"), nil},
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
			helper.dummyTopics("a", "b"),
			helper.dummyTopic("a"),
			helper.dummyTopics("b"),
		},
		{
			helper.dummyTopics("a", "b"),
			helper.dummyTopic("c"),
			helper.dummyTopics("a", "b"),
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
		topic := helper.dummyTopic("a")
		topic.store = newTestDatastore()
		topic.subscriptions = []Subscription{}
		got := topic.Publish(c.inputData, c.inputAttr)
		if got != c.expectErr {
			t.Errorf("%#d: want %v, got %v", i, c.expectErr, got)
		}
	}
}
