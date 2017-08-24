package client

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/takashabe/go-message-queue/server"
)

func setupServer(t *testing.T) *httptest.Server {
	s, err := server.NewServer("testdata/config.yaml")
	if err != nil {
		t.Fatalf("failed to server.NewServer, error=%v", err)
	}
	if err := s.InitDatastore(); err != nil {
		t.Fatalf("failed to server.InitDatastore, error=%v", err)
	}
	return httptest.NewServer(server.Routes())
}

func createDummyTopics(t *testing.T, ts *httptest.Server) {
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("failed to NewClient, error=%v", err)
	}
	for _, id := range []string{"topic1", "topic2"} {
		_, err := client.CreateTopic(ctx, id)
		if err != nil {
			t.Fatalf("failed to create new topic, error=%v", err)
		}
	}
}
func TestCreateTopic(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()

	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("failed to NewClient, error=%v", err)
	}

	cases := []struct {
		input  string
		expect *Topic
	}{
		{"a", &Topic{id: "a", s: client.s}},
		{"b", &Topic{id: "b", s: client.s}},
	}
	for i, c := range cases {
		topic, err := client.CreateTopic(ctx, c.input)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !reflect.DeepEqual(c.expect, topic) {
			t.Errorf("#%d: want %v, got %v", i, c.expect, topic)
		}
	}
}

func TestPublish(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	createDummyTopics(t, ts)
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}

	cases := []struct {
		inputs []*Message
	}{
		{
			[]*Message{
				&Message{Data: []byte(`msg1`)},
				&Message{Data: []byte(`msg2`), Attributes: map[string]string{"msg2": "foo"}},
			},
		},
		{
			[]*Message{
				&Message{Data: []byte(`msg3`)},
				&Message{},
			},
		},
	}
	for i, c := range cases {
		// asynchronously publish messages
		topic := client.Topic("topic1")
		idCh := make(chan string)
		for mi, m := range c.inputs {
			go func(ci, gi int, gm *Message) {
				result := topic.Publish(ctx, gm)
				msgID, err := result.Get(ctx)
				if err != nil {
					idCh <- ""
					t.Fatalf("#%d-%d: failed to publish message, error=%v", ci, gi, err)
				}
				idCh <- msgID
			}(i, mi, m)
		}

		for cntCh := 0; cntCh < len(c.inputs); cntCh++ {
			if msgID := <-idCh; msgID == "" {
				t.Errorf("#%d-%d: want non-empty message id", i, cntCh)
			}
		}
	}
}
