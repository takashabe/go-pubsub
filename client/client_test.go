package client

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

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

func createDummySubscriptions(t *testing.T, ts *httptest.Server, topic *Topic) {
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("failed to NewClient, error=%v", err)
	}
	for _, id := range []string{"sub1", "sub2"} {
		client.CreateSubscription(ctx, id, SubscriptionConfig{
			Topic:      topic,
			AckTimeout: time.Second,
		})
	}
}

func publishDummyMessage(t *testing.T, topic *Topic) []string {
	return publishMessages(t, topic, []*Message{
		&Message{Data: []byte(`msg1`)},
		&Message{Data: []byte(`msg2`), Attributes: map[string]string{"msg2": "foo"}},
	})
}

func publishMessages(t *testing.T, topic *Topic, msgs []*Message) []string {
	ctx := context.Background()
	msgIDs := []string{}
	for _, msg := range msgs {
		result := topic.Publish(ctx, msg)
		id, err := result.Get(ctx)
		if err != nil {
			t.Fatalf("failed to publish message, error=%v", err)
		}
		msgIDs = append(msgIDs, id)
	}
	if len(msgIDs) != len(msgs) {
		t.Fatalf("want message size %d, got %d", len(msgs), len(msgIDs))
	}
	return msgIDs
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

		exists, err := topic.Exists(ctx)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !exists {
			t.Fatalf("#%d: want exists, but not exists", i)
		}
	}
}

func TestCreateSubscription(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	createDummyTopics(t, ts)

	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("failed to NewClient, error=%v", err)
	}

	cases := []struct {
		inputID  string
		inputCfg SubscriptionConfig
		expect   *Subscription
	}{
		{
			"sub1",
			SubscriptionConfig{
				Topic: client.Topic("topic1"),
			},
			&Subscription{id: "sub1", s: client.s},
		},
	}
	for i, c := range cases {
		sub, err := client.CreateSubscription(ctx, c.inputID, c.inputCfg)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !reflect.DeepEqual(c.expect, sub) {
			t.Errorf("#%d: want %v, got %v", i, c.expect, sub)
		}

		exists, err := sub.Exists(ctx)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !exists {
			t.Fatalf("#%d: want exists, but not exists", i)
		}

		subs, err := c.inputCfg.Topic.Subscriptions(ctx)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		contain := false
		for _, s := range subs {
			if s.id == sub.id {
				contain = true
			}
		}
		if !contain {
			t.Errorf("#%d: want Subscriptions contain %s", i, sub.id)
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

func TestReceiveAndAck(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	createDummyTopics(t, ts)
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	createDummySubscriptions(t, ts, client.Topic("topic1"))

	cases := []struct {
		inputs []*Message
	}{
		{
			[]*Message{
				&Message{Data: []byte(`msg1`)},
				&Message{Data: []byte(`msg2`), Attributes: map[string]string{"msg2": "foo"}},
			},
		},
	}
	for i, c := range cases {
		msgIDs := publishMessages(t, client.Topic("topic1"), c.inputs)

		sub := client.Subscription("sub1")
		ackIDs := []string{}
		for j := 0; j < len(msgIDs); j++ {
			err := sub.Receive(ctx, func(ctx context.Context, msg *Message) {
				// expect: the received message ID exists in recently published messages
				contain := false
				for _, pid := range msgIDs {
					if msg.ID == pid {
						contain = true
						break
					}
				}
				if !contain {
					t.Errorf("#%d: want message id %s contain publish messaged", i, msg.ID)
				}

				// collect AckID
				ackIDs = append(ackIDs, msg.AckID)
			})
			if err != nil {
				t.Fatalf("#%d: want non-error, got %v", i, err)
			}
		}

		err := sub.Ack(ctx, ackIDs)
		if err != nil {
			t.Fatalf("#%d: want non-error, got %v", i, err)
		}
	}
}
