package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
)

func TestCreateSubscription(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopics(t, ts)

	cases := []struct {
		inputName  string
		inputBody  interface{}
		expectCode int
		expectBody []byte
	}{
		{
			"A",
			ResourceSubscription{
				Topic: "a",
				Push: PushConfig{
					Endpoint: "test",
					Attr:     map[string]string{"1": "2"},
				},
				AckTimeout: 10,
			},
			http.StatusCreated,
			[]byte(`{"topic":"a","push_config":{"endpoint":"test","attributes":{"1":"2"}},"ack_deadline_seconds":10}`),
		},
		{
			"A",
			ResourceSubscription{
				Topic: "a",
				Push: PushConfig{
					Endpoint: "test",
					Attr:     map[string]string{"1": "2"},
				},
				AckTimeout: 10,
			},
			http.StatusNotFound,
			[]byte(`{"reason":"failed to create subscription"}`),
		},
		{
			"B",
			ResourceSubscription{
				Topic:      "a",
				AckTimeout: 10,
			},
			http.StatusCreated,
			[]byte(`{"topic":"a","push_config":{"endpoint":"","attributes":null},"ack_deadline_seconds":10}`),
		},
		{
			"C",
			"",
			http.StatusNotFound,
			[]byte(`{"reason":"failed to parsed request"}`),
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		b, err := json.Marshal(c.inputBody)
		if err != nil {
			t.Fatalf("#%d: failed to encode json", i)
		}
		req, err := http.NewRequest("PUT",
			fmt.Sprintf("%s/subscription/create/%s", ts.URL, c.inputName), bytes.NewBuffer(b))
		if err != nil {
			t.Fatalf("#%d: failed to create request", i)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatalf("#%d: failed to send request", i)
		}
		defer res.Body.Close()

		if got := res.StatusCode; got != c.expectCode {
			t.Errorf("#%d: want %d, got %d", i, c.expectCode, got)
		}
		got, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("#%d: failed to read body, got err %v", i, err)
		}
		if !reflect.DeepEqual(got, c.expectBody) {
			t.Errorf("#%d: want %s, got %s", i, c.expectBody, got)
		}
	}
}

func TestGetSubscription(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopicAndSub(t, ts)

	cases := []struct {
		input      string
		expectCode int
		expectBody []byte
	}{
		{
			"A",
			http.StatusOK,
			[]byte(`{"topic":"a","push_config":{"endpoint":"","attributes":null},"ack_deadline_seconds":10}`),
		},
		{
			"C",
			http.StatusNotFound,
			[]byte(`{"reason":"not found subscription"}`),
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		res, err := client.Get(fmt.Sprintf("%s/subscription/get/%s", ts.URL, c.input))
		if err != nil {
			t.Fatalf("#%d: failed to send request", i)
		}
		defer res.Body.Close()

		if got := res.StatusCode; got != c.expectCode {
			t.Errorf("#%d: want %d, got %d", i, c.expectCode, got)
		}
		got, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("#%d: failed to read body, got err %v", i, err)
		}
		if !reflect.DeepEqual(got, c.expectBody) {
			t.Errorf("#%d: want %s, got %s", i, c.expectBody, got)
		}
	}
}
