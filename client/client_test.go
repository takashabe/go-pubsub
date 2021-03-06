package client

import (
	"context"
	"fmt"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/takashabe/go-pubsub/server"
)

func setupServer(t *testing.T) *httptest.Server {
	s, err := server.NewServer("testdata/config.yaml")
	if err != nil {
		t.Fatalf("failed to server.NewServer, error=%v", err)
	}
	if err := s.PrepareServer(); err != nil {
		t.Fatalf("failed to PrepareServer, error=%v", err)
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
		{"a", &Topic{ID: "a", s: client.s}},
		{"b", &Topic{ID: "b", s: client.s}},
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
			&Subscription{ID: "sub1", s: client.s},
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
			if s.ID == sub.ID {
				contain = true
			}
		}
		if !contain {
			t.Errorf("#%d: want Subscriptions contain %s", i, sub.ID)
		}
	}
}

func TestListTopics(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non error, got %v", err)
	}

	cases := []struct {
		input  []string
		expect []*Topic
	}{
		{
			[]string{"a", "b"},
			[]*Topic{client.Topic("a"), client.Topic("b")},
		},
		{
			[]string{"c"},
			[]*Topic{client.Topic("a"), client.Topic("b"), client.Topic("c")},
		},
	}
	for i, c := range cases {
		for _, id := range c.input {
			_, err := client.CreateTopic(ctx, id)
			if err != nil {
				t.Fatalf("#%d: want non error, got %v", i, err)
			}
		}

		topics, err := client.Topics(ctx)
		sort.Sort(ByTopicID(topics))
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !reflect.DeepEqual(topics, c.expect) {
			t.Errorf("#%d: want topic list %v, got %v", i, c.expect, topics)
		}
	}
}

func TestListSubscription(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non error, got %v", err)
	}
	createDummyTopics(t, ts)

	cases := []struct {
		inputSubs     []string
		inputTopic    string
		expectAll     []*Subscription
		expectInTopic []*Subscription
	}{
		{
			[]string{"a", "b"},
			"topic1",
			[]*Subscription{client.Subscription("a"), client.Subscription("b")},
			[]*Subscription{client.Subscription("a"), client.Subscription("b")},
		},
		{
			[]string{"c"},
			"topic2",
			[]*Subscription{client.Subscription("a"), client.Subscription("b"), client.Subscription("c")},
			[]*Subscription{client.Subscription("c")},
		},
	}
	for i, c := range cases {
		for _, id := range c.inputSubs {
			_, err := client.CreateSubscription(ctx, id, SubscriptionConfig{
				Topic: client.Topic(c.inputTopic),
			})
			if err != nil {
				t.Fatalf("#%d: want non error, got %v", i, err)
			}
		}

		// check all subscriptions
		subs, err := client.Subscriptions(ctx)
		sort.Sort(BySubscriptionID(subs))
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !reflect.DeepEqual(subs, c.expectAll) {
			t.Errorf("#%d: want Subscription list %v, got %v", i, c.expectAll, subs)
		}

		// check subscriptions in the topic
		subs, err = client.Topic(c.inputTopic).Subscriptions(ctx)
		sort.Sort(BySubscriptionID(subs))
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !reflect.DeepEqual(subs, c.expectInTopic) {
			t.Errorf("#%d: want topic list %v, got %v", i, c.expectInTopic, subs)
		}
	}
}

func TestDeleteTopic(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	createDummyTopics(t, ts)
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}

	// delete topic1
	topic1 := client.Topic("topic1")
	err = topic1.Delete(ctx)
	if err != nil {
		t.Fatalf("want non error, got %v", err)
	}

	// check listTopics
	expect := []*Topic{
		client.Topic("topic2"),
	}
	topics, err := client.Topics(ctx)
	if err != nil {
		t.Fatalf("want non error, got %v", err)
	}
	if !reflect.DeepEqual(expect, topics) {
		t.Errorf("want topic list %v, got %v", expect, topics)
	}
}

func TestDeleteSubscription(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	createDummyTopics(t, ts)
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	createDummySubscriptions(t, ts, client.Topic("topic1"))

	// delete sub1
	sub1 := client.Subscription("sub1")
	err = sub1.Delete(ctx)
	if err != nil {
		t.Fatalf("want non error, got %v", err)
	}

	// check listSubscriptions
	expect := []*Subscription{
		client.Subscription("sub2"),
	}
	subs, err := client.Subscriptions(ctx)
	if err != nil {
		t.Fatalf("want non error, got %v", err)
	}
	if !reflect.DeepEqual(expect, subs) {
		t.Errorf("want subscription list %v, got %v", expect, subs)
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

func TestAckAndNack(t *testing.T) {
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
		fn func(sub *Subscription, ackIDs []string)
	}{
		{
			func(sub *Subscription, ackIDs []string) {
				err := sub.Ack(ctx, ackIDs)
				if err != nil {
					t.Errorf("want non error, got %v", err)
				}
				// expect can't pull message after the Ack
				err = sub.Receive(ctx, func(ctx context.Context, msg *Message) {})
				if err != ErrNotFoundMessage {
					t.Errorf("want error %v, got %v", ErrNotFoundMessage, err)
				}
			},
		},
		{
			func(sub *Subscription, ackIDs []string) {
				err := sub.Nack(ctx, ackIDs)
				if err != nil {
					t.Errorf("want non error, got %v", err)
				}
				// expect can't pull message after the Ack
				err = sub.Receive(ctx, func(ctx context.Context, msg *Message) {})
				if err != nil {
					t.Errorf("want non error, got %v", err)
				}
			},
		},
	}
	for i, c := range cases {
		sub, err := client.CreateSubscription(ctx, fmt.Sprintf("sub-%d", i), SubscriptionConfig{
			Topic: client.Topic("topic1"),
		})
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}

		// publish and receive one message
		publishMessages(t, client.Topic("topic1"), []*Message{&Message{Data: []byte(`msg1`)}})
		var ackIDs []string
		sub.Receive(ctx, func(ctx context.Context, msg *Message) {
			ackIDs = []string{msg.AckID}
		})

		c.fn(sub, ackIDs)
	}
}

func TestConfigUpdateSubscription(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	createDummyTopics(t, ts)
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	createDummySubscriptions(t, ts, client.Topic("topic1"))

	sub := client.Subscription("sub1")
	originConf, err := sub.Config(ctx)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}

	toUpdate := &PushConfig{
		Endpoint:   ts.URL,
		Attributes: map[string]string{"a": "b"},
	}
	err = sub.Update(ctx, &SubscriptionConfigToUpdate{PushConfig: toUpdate})
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	updatedConf, err := sub.Config(ctx)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}

	if reflect.DeepEqual(originConf, updatedConf) {
		t.Errorf("want differ config from in before and after update")
	}
	expectUpdateConf := originConf
	expectUpdateConf.PushConfig = updatedConf.PushConfig
	if !reflect.DeepEqual(expectUpdateConf, updatedConf) {
		t.Errorf("want update config %v, got %v", expectUpdateConf, updatedConf)
	}
}

func TestStatsSummary(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	createDummyTopics(t, ts)
	createDummySubscriptions(t, ts, client.Topic("topic1"))
	publishDummyMessage(t, client.Topic("topic1"))

	expect := []byte(`{"topic.topic_num":2.0,"subscription.subscription_num":2.0,"topic.message_count":2.0,"subscription.message_count":4.0}`)
	payload, err := client.Stats(ctx)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	if !reflect.DeepEqual(expect, payload) {
		t.Errorf("want payload %s, got %s", expect, payload)
	}
}

func TestStatsTopicDetail(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	createDummyTopics(t, ts)
	createDummySubscriptions(t, ts, client.Topic("topic1"))
	publishDummyMessage(t, client.Topic("topic1"))

	expect := []byte(`"topic.topic1.message_count":2.0`)
	payload, err := client.Topic("topic1").StatsDetail(ctx)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	if !strings.Contains(string(payload), string(expect)) {
		t.Errorf("want contain %s, got %s", expect, payload)
	}
}

func TestStatsSubscriptionDetail(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	ctx := context.Background()
	client, err := NewClient(ctx, ts.URL)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	createDummyTopics(t, ts)
	createDummySubscriptions(t, ts, client.Topic("topic1"))
	msgIDs := publishDummyMessage(t, client.Topic("topic1"))

	expect := []byte(fmt.Sprintf("\"subscription.sub1.current_messages\":[\"%s\",\"%s\"]", msgIDs[0], msgIDs[1]))
	payload, err := client.Subscription("sub1").StatsDetail(ctx)
	if err != nil {
		t.Fatalf("want non-error, got %v", err)
	}
	if !strings.Contains(string(payload), string(expect)) {
		t.Errorf("want contain %s, got %s", expect, payload)
	}
}
