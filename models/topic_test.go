package models

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/takashabe/go-pubsub/datastore"
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
		setupDatastore(t)
		var err error
		for _, s := range c.inputs {
			// expect last input return value equal expectErr
			_, err = NewTopic(s)
		}
		if err != c.expectErr {
			t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		}

		list, err := globalTopics.List()
		if err != nil {
			t.Fatalf("#%d: want no error, got %v", i, err)
		}
		if len(list) != len(c.expectExistTopics) {
			t.Fatalf("#%d: want %d, got %d", i, len(c.expectExistTopics), len(list))
		}
		for i2, s := range c.expectExistTopics {
			if _, err = globalTopics.Get(s); err != nil {
				t.Errorf("#%d-%d: key %s want no error, got %v", i, i2, s, err)
			}
		}
	}
}

func TestGetTopic(t *testing.T) {
	setupDatastore(t)
	setupDummyTopics(t)

	cases := []struct {
		input           string
		expectTopicName string
		expectErr       error
	}{
		{"A", "A", nil},
		{"D", "", datastore.ErrNotFoundEntry},
	}
	for i, c := range cases {
		got, err := GetTopic(c.input)
		if errors.Cause(err) != c.expectErr {
			t.Fatalf("#%d: want %v, got %v", i, c.expectErr, err)
		}
		if c.expectTopicName != "" && c.expectTopicName != got.Name {
			t.Errorf("#%d: want %s, got %s", i, c.expectTopicName, got.Name)
		}
	}
}
