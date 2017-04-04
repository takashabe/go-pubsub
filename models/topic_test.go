package queue

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"
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

		list, err := globalTopics.List()
		if err != nil {
			t.Fatalf("%#d: want no error, got %v", i, err)
		}
		if len(list) != len(c.expectExistTopics) {
			t.Fatalf("%#d: want %d, got %d", len(c.expectExistTopics), len(list))
		}
		for i2, s := range c.expectExistTopics {
			if _, err = globalTopics.Get(s); err != nil {
				t.Errorf("#%d-%d: key %s want no error, got %v", i, i2, s, err)
			}
		}
	}
}

func TestGetTopic(t *testing.T) {
	// make test topics
	helper.setupGlobal()
	globalTopics.Set(helper.dummyTopic(t, "a"))
	globalTopics.Set(helper.dummyTopic(t, "b"))

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
		if errors.Cause(err) != c.expectErr {
			t.Errorf("%#d: want %v, got %v", i, c.expectErr, err)
		}
		if !reflect.DeepEqual(got, c.expectTopic) {
			t.Errorf("%#d: want %v, got %v", i, c.expectTopic, got)
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
