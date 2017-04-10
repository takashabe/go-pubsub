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

func TestCreateAndGetTopic(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()

	cases := []struct {
		input         string
		expectPutCode int
		expectGetCode int
		expectPutBody []byte
		expectGetBody []byte
	}{
		{
			"A",
			http.StatusCreated, http.StatusOK,
			[]byte(`{"name":"A"}`),
			[]byte(`{"name":"A"}`),
		},
		{
			"A",
			http.StatusNotFound, http.StatusOK,
			[]byte(`{"reason":"failed to create topic"}`),
			[]byte(`{"name":"A"}`),
		},
	}
	for i, c := range cases {
		// create
		client := dummyClient(t)
		create, err := http.NewRequest("PUT", ts.URL+"/topic/create/"+c.input, nil)
		if err != nil {
			t.Fatalf("#%d: failed to create request, %v", i, err)
		}
		res, err := client.Do(create)
		if err != nil {
			t.Fatalf("#%d: failed to send request, %v", i, err)
		}
		defer res.Body.Close()
		if got := res.StatusCode; c.expectPutCode != got {
			t.Errorf("#%d: want %d, got %d", i, c.expectPutCode, got)
		}
		if got, _ := ioutil.ReadAll(res.Body); !reflect.DeepEqual(got, c.expectPutBody) {
			t.Errorf("#%d: want %s, got %s", i, c.expectPutBody, got)
		}

		// get
		get, err := http.NewRequest("GET", ts.URL+"/topic/get/"+c.input, nil)
		if err != nil {
			t.Fatalf("#%d: failed to create request, %v", i, err)
		}
		res, err = client.Do(get)
		if err != nil {
			t.Fatalf("#%d: failed to request, %v", i, err)
		}
		defer res.Body.Close()
		if got := res.StatusCode; c.expectGetCode != got {
			t.Errorf("#%d: want %d, got %d", i, c.expectGetCode, got)
		}
		if got, _ := ioutil.ReadAll(res.Body); !reflect.DeepEqual(got, c.expectGetBody) {
			t.Errorf("#%d: want %s, got %s", i, c.expectGetBody, got)
		}
	}
}

func TestDelete(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopics(t, ts)

	cases := []struct {
		input      string
		expectCode int
		expectBody []byte
	}{
		{"a", http.StatusNoContent, []byte("")},
		{"a", http.StatusNotFound, []byte(`{"reason":"topic already not exist"}`)},
	}
	for i, c := range cases {
		client := dummyClient(t)
		req, err := http.NewRequest("DELETE", ts.URL+"/topic/delete/"+c.input, nil)
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
		if got, _ := ioutil.ReadAll(res.Body); !reflect.DeepEqual(got, c.expectBody) {
			t.Errorf("#%d: want %s, got %s", i, c.expectBody, got)
		}
	}
}

func TestListTopic(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopics(t, ts)

	client := dummyClient(t)
	req, err := http.NewRequest("GET", ts.URL+"/topic/list", nil)
	if err != nil {
		t.Fatal("failed to create request")
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal("failed to send request")
	}
	defer res.Body.Close()

	var want interface{}
	want = http.StatusOK
	if got := res.StatusCode; got != want {
		t.Errorf("want %d, got %d", want, got)
	}
	want = []byte(`[{"name":"a"},{"name":"b"},{"name":"c"}]`)
	if got, _ := ioutil.ReadAll(res.Body); !reflect.DeepEqual(got, want) {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestListTopicSubscription(t *testing.T) {
	// TODO: implements test, after the subscription testing finished
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopics(t, ts)

	cases := []struct {
		input      string
		expectCode int
		expectBody []byte
	}{
		{"a", http.StatusOK, []byte(`[]`)},
	}
	for i, c := range cases {
		client := dummyClient(t)
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/topic/%s/subscriptions", ts.URL, c.input), nil)
		if err != nil {
			t.Fatal("failed to create request")
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal("failed to send request")
		}
		defer res.Body.Close()

		if got := res.StatusCode; got != c.expectCode {
			t.Errorf("#%d: want %d, got %d", i, c.expectCode, got)
		}
		if got, _ := ioutil.ReadAll(res.Body); !reflect.DeepEqual(got, c.expectBody) {
			t.Errorf("#%d: want %s, got %s", i, c.expectBody, got)
		}
	}
}

func TestPublish(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopics(t, ts)

	cases := []struct {
		input          interface{}
		expectCode     int
		expectMsgCount int
	}{
		{
			PublishDatas{
				Messages: []PublishData{
					PublishData{Data: []byte(`test1`), Attr: nil},
					PublishData{Data: []byte(`test2`), Attr: map[string]string{"1": "2"}},
				},
			},
			http.StatusOK,
			2,
		},
		{
			"",
			http.StatusNotFound,
			0,
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		b, err := json.Marshal(c.input)
		if err != nil {
			t.Fatal("failed to encode to json")
		}
		req, err := http.NewRequest("POST", fmt.Sprintf("%s/topic/a/publish", ts.URL), bytes.NewBuffer(b))
		if err != nil {
			t.Fatal("failed to create request")
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal("failed to send request")
		}
		defer res.Body.Close()

		if got := res.StatusCode; got != c.expectCode {
			t.Errorf("#%d: want %d, got %d", i, c.expectCode, got)
		}
		var msgs ResponsePublish
		if err := json.NewDecoder(res.Body).Decode(&msgs); err != nil {
			body, _ := ioutil.ReadAll(res.Body)
			t.Fatalf("#%d: failed to decode response body, body=%v", i, body)
		}
		if got := len(msgs.MessageIDs); got != c.expectMsgCount {
			t.Errorf("#%d: want %d, got %d, got json %v", i, c.expectCode, got, msgs)
		}
	}
}
