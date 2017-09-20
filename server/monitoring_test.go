package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestSummary(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()

	cases := []struct {
		prepare func(client *http.Client)
		expect  []byte
	}{
		{
			func(client *http.Client) {
				createDummyTopic(t, ts, "topic1")
				createDummyTopic(t, ts, "topic2")
			},
			[]byte(`{"topic.topic_num":2.0,"subscription.subscription_num":0.0,"topic.message_count":0.0,"subscription.message_count":0.0}`),
		},
		{
			func(client *http.Client) {
				createDummyTopic(t, ts, "topic3")
				setupPublishMessages(t, ts, "topic3", PublishDatas{
					Messages: []PublishData{
						PublishData{Data: []byte(`test1`), Attr: nil},
						PublishData{Data: []byte(`test2`), Attr: map[string]string{"1": "2"}},
					},
				})
			},
			[]byte(`{"topic.topic_num":3.0,"subscription.subscription_num":0.0,"topic.message_count":2.0,"subscription.message_count":0.0}`),
		},
		{
			func(client *http.Client) {
				createDummySubscription(t, ts, ResourceSubscription{
					Topic:      "topic1",
					Name:       "sub1",
					AckTimeout: 1,
				})
				setupPublishMessages(t, ts, "topic1", PublishDatas{
					Messages: []PublishData{PublishData{Data: []byte(`test1`), Attr: nil}},
				})
			},
			[]byte(`{"topic.topic_num":3.0,"subscription.subscription_num":1.0,"topic.message_count":3.0,"subscription.message_count":1.0}`),
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		c.prepare(client)

		res, err := client.Get(ts.URL + "/stats")
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		defer res.Body.Close()

		payload, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !reflect.DeepEqual(c.expect, payload) {
			t.Errorf("#%d: want response payload %s, got %s", i, c.expect, payload)
		}
	}
}

func TestTopicSummary(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()

	cases := []struct {
		prepare func(client *http.Client)
		expect  []byte
	}{
		{
			func(client *http.Client) {
				createDummyTopic(t, ts, "topic1")
				createDummyTopic(t, ts, "topic2")
			},
			[]byte(`{"topic.topic_num":2.0,"topic.message_count":0.0}`),
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		c.prepare(client)

		res, err := client.Get(ts.URL + "/stats/topic")
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		defer res.Body.Close()

		payload, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !reflect.DeepEqual(c.expect, payload) {
			t.Errorf("#%d: want response payload %s, got %s", i, c.expect, payload)
		}
	}
}

func TestSubscriptionSummary(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()

	cases := []struct {
		prepare func(client *http.Client)
		expect  []byte
	}{
		{
			func(client *http.Client) {
				createDummyTopic(t, ts, "topic1")
				createDummySubscription(t, ts, ResourceSubscription{
					Topic:      "topic1",
					Name:       "sub1",
					AckTimeout: 1,
				})
				createDummySubscription(t, ts, ResourceSubscription{
					Topic:      "topic1",
					Name:       "sub2",
					AckTimeout: 1,
				})
				setupPublishMessages(t, ts, "topic1", PublishDatas{
					Messages: []PublishData{
						PublishData{Data: []byte(`test1`)},
						PublishData{Data: []byte(`test2`)},
					},
				})
			},
			[]byte(`{"subscription.subscription_num":2.0,"subscription.message_count":4.0}`),
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		c.prepare(client)

		res, err := client.Get(ts.URL + "/stats/subscription")
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		defer res.Body.Close()

		payload, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		if !reflect.DeepEqual(c.expect, payload) {
			t.Errorf("#%d: want response payload %s, got %s", i, c.expect, payload)
		}
	}
}

func TestTopicDetail(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()

	cases := []struct {
		prepare func(client *http.Client)
		target  string
		expect  func(startTime time.Time, res *http.Response)
	}{
		{
			func(client *http.Client) {
				createDummyTopic(t, ts, "topic1")
				createDummyTopic(t, ts, "topic2")
				setupPublishMessages(t, ts, "topic1", PublishDatas{
					Messages: []PublishData{
						PublishData{Data: []byte(`test1`)},
						PublishData{Data: []byte(`test2`)},
					},
				})
			},
			"topic1",
			func(startTime time.Time, res *http.Response) {
				type MetricJSON struct {
					CreatedAt    float64 `json:"topic.topic1.created_at"`
					MessageCount float64 `json:"topic.topic1.message_count"`
				}
				var j MetricJSON
				err := json.NewDecoder(res.Body).Decode(&j)
				if err != nil {
					t.Fatalf("want non error, got %v", err)
				}

				// check period for the created_at
				createTime := int64(j.CreatedAt)
				now := time.Now().Unix()
				if startTime.Unix() > createTime || now < createTime {
					t.Errorf("want createTime between startTime and Now, createTime:%v, startTime:%v, Now:%v",
						createTime, startTime.Unix(), now)
				}
				// check num for the message_count
				actCnt := int(j.MessageCount)
				if cnt := 2; actCnt != cnt {
					t.Errorf("want message_count %d, got %d", cnt, actCnt)
				}
			},
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		c.prepare(client)

		startTime := time.Now()
		res, err := client.Get(ts.URL + "/stats/topic/" + c.target)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		defer res.Body.Close()

		c.expect(startTime, res)
	}
}

func TestSubscriptionDetail(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()

	cases := []struct {
		prepare func(client *http.Client)
		target  string
		expect  func(startTime time.Time, res *http.Response)
	}{
		{
			func(client *http.Client) {
				createDummyTopic(t, ts, "topic1")
				createDummySubscription(t, ts, ResourceSubscription{
					Topic:      "topic1",
					Name:       "sub1",
					AckTimeout: 1,
				})
				createDummySubscription(t, ts, ResourceSubscription{
					Topic:      "topic1",
					Name:       "sub2",
					AckTimeout: 1,
				})
				setupPublishMessages(t, ts, "topic1", PublishDatas{
					Messages: []PublishData{
						PublishData{Data: []byte(`test1`)},
						PublishData{Data: []byte(`test2`)},
					},
				})
			},
			"sub1",
			func(startTime time.Time, res *http.Response) {
				type MetricJSON struct {
					CreatedAt    float64 `json:"subscription.sub1.created_at"`
					MessageCount float64 `json:"subscription.sub1.message_count"`
				}
				var j MetricJSON
				err := json.NewDecoder(res.Body).Decode(&j)
				if err != nil {
					t.Fatalf("want non error, got %v", err)
				}

				// check period for the created_at
				createTime := int64(j.CreatedAt)
				now := time.Now().Unix()
				if startTime.Unix() > createTime || now < createTime {
					t.Errorf("want createTime between startTime and Now, createTime:%v, startTime:%v, Now:%v",
						createTime, startTime.Unix(), now)
				}
				// check num for the message_count
				actCnt := int(j.MessageCount)
				if cnt := 2; actCnt != cnt {
					t.Errorf("want message_count %d, got %d", cnt, actCnt)
				}
			},
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		c.prepare(client)

		startTime := time.Now()
		res, err := client.Get(ts.URL + "/stats/subscription/" + c.target)
		if err != nil {
			t.Fatalf("#%d: want non error, got %v", i, err)
		}
		defer res.Body.Close()

		c.expect(startTime, res)
	}
}
