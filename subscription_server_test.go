package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"
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
			[]byte(`{"name":"A","topic":"a","push_config":{"endpoint":"test","attributes":{"1":"2"}},"ack_deadline_seconds":10}`),
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
			[]byte(`{"name":"B","topic":"a","push_config":{"endpoint":"","attributes":null},"ack_deadline_seconds":10}`),
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
			fmt.Sprintf("%s/subscription/%s", ts.URL, c.inputName), bytes.NewBuffer(b))
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
			[]byte(`{"name":"A","topic":"a","push_config":{"endpoint":"","attributes":null},"ack_deadline_seconds":10}`),
		},
		{
			"C",
			http.StatusNotFound,
			[]byte(`{"reason":"not found subscription"}`),
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		res, err := client.Get(fmt.Sprintf("%s/subscription/%s", ts.URL, c.input))
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

func TestDeleteSubscription(t *testing.T) {
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
			http.StatusNoContent,
			[]byte(``),
		},
		{
			"A",
			http.StatusNotFound,
			[]byte(`{"reason":"subscription already not exist"}`),
		},
	}
	for i, c := range cases {
		client := dummyClient(t)
		req, err := http.NewRequest("DELETE",
			fmt.Sprintf("%s/subscription/%s", ts.URL, c.input), nil)
		if err != nil {
			t.Fatalf("#%d: failed to create request", i)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatalf("#%d: failed to send request", i)
		}
		defer res.Body.Close()

		if got := res.StatusCode; got != c.expectCode {
			t.Fatalf("#%d: want %d, got %d", i, c.expectCode, got)
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

func TestListSubscription(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopicAndSub(t, ts)

	client := dummyClient(t)
	res, err := client.Get(fmt.Sprintf("%s/subscription/", ts.URL))
	if err != nil {
		t.Fatalf("failed to send request, got err %v", err)
	}
	defer res.Body.Close()

	wantCode := http.StatusOK
	if got := res.StatusCode; got != wantCode {
		t.Errorf("want %d, got %d", wantCode, got)
	}
	// want sub names
	wantNames := []string{"A", "B"}
	var subs []ResourceSubscription
	if err := json.NewDecoder(res.Body).Decode(&subs); err != nil {
		body, _ := ioutil.ReadAll(res.Body)
		t.Fatalf("failed to decode response body, body=%v", body)
	}
	for i, n := range wantNames {
		if subs[i].Name != n {
			t.Errorf("#%d: want %s, got %s, got sub %v", i, n, subs[i].Name, subs[i])
		}
	}
}

func TestPull(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopicAndSub(t, ts)
	dummyPublishMessage(t, ts)

	cases := []struct {
		inputName  string
		inputBody  interface{}
		expectCode int
		expectSize int
	}{
		{
			"A",
			RequestPull{MaxMessages: 2},
			http.StatusOK,
			2,
		},
		{
			"A",
			RequestPull{MaxMessages: 3},
			http.StatusOK,
			1,
		},
		{
			"A",
			RequestPull{MaxMessages: 3},
			http.StatusNotFound,
			0,
		},
	}
	for i, c := range cases {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(c.inputBody); err != nil {
			t.Fatal("#%d: failed to encode struct")
		}
		client := dummyClient(t)
		res, err := client.Post(
			fmt.Sprintf("%s/subscription/%s/pull", ts.URL, c.inputName),
			"application/json", &buf)
		if err != nil {
			t.Fatalf("failed to send request, got err %v", err)
		}
		defer res.Body.Close()

		if got := res.StatusCode; got != c.expectCode {
			if b, err := ioutil.ReadAll(res.Body); err != nil {
				t.Fatalf("#%d: code want %d, got %d", i, c.expectCode, got)
			} else {
				t.Fatalf("#%d: code want %d, got %d, body %s", i, c.expectCode, got, b)
			}
		}
		var body ResponsePull
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			actBody, _ := ioutil.ReadAll(res.Body)
			t.Fatalf("#%d: failed decode to json, body %s", i, actBody)
		}
		if got := len(body.Messages); got != c.expectSize {
			t.Errorf("#%d: message len want %d, got %d", i, c.expectSize, got)
		}
	}
}

// testing for ack response
func TestAck(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopicAndSub(t, ts)
	dummyPublishMessage(t, ts)

	// beforehand pull message
	response := pullMessage(t, ts, "A", 1)
	defer response.Body.Close()
	var responsePull ResponsePull
	if err := json.NewDecoder(response.Body).Decode(&responsePull); err != nil {
		t.Fatalf("failed to beforehand encode json, got err %v", err)
	}
	ackIDs := make([]string, 0)
	for _, r := range responsePull.Messages {
		ackIDs = append(ackIDs, r.AckID)
	}

	cases := []struct {
		inputBody  interface{}
		expectCode int
	}{
		{RequestAck{AckIDs: ackIDs}, http.StatusOK},
		{RequestAck{AckIDs: ackIDs}, http.StatusNotFound}, // used ackID want error
		{"", http.StatusNotFound},
	}
	for i, c := range cases {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(c.inputBody); err != nil {
			t.Fatalf("#%d: failed to encode json, got err %v", i, err)
		}
		client := dummyClient(t)
		res, err := client.Post(
			fmt.Sprintf("%s/subscription/%s/ack", ts.URL, "A"),
			"application/json", &buf)
		if err != nil {
			t.Fatalf("#%d: failed to send request, got err %v", i, err)
		}
		defer res.Body.Close()
		if got := res.StatusCode; got != c.expectCode {
			t.Errorf("#%d: code want %d, got %d", i, c.expectCode, got)
		}
	}
}

// testing for ack timeout
func TestPullAck(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopics(t, ts)
	hackCreateShortAckSubscription(t)
	dummyPublishMessage(t, ts)

	cases := []struct {
		inputSize  int
		expectSize int
		expectCode int
		isAck      bool
		sleep      time.Duration
	}{
		{2, 2, http.StatusOK, false, 100 * time.Millisecond},
		{2, 2, http.StatusOK, true, 100 * time.Millisecond},
		{2, 1, http.StatusOK, true, 0 * time.Millisecond},
		{2, 0, http.StatusNotFound, false, 0 * time.Millisecond},
	}
	for i, c := range cases {
		// scenario: pull -> (ack) -> sleep -> pull ...
		res := pullMessage(t, ts, "A", c.inputSize)
		defer res.Body.Close()
		if got := res.StatusCode; got != c.expectCode {
			if b, err := ioutil.ReadAll(res.Body); err != nil {
				t.Fatalf("#%d: code want %d, got %d", i, c.expectCode, got)
			} else {
				t.Fatalf("#%d: code want %d, got %d, body %s", i, c.expectCode, got, b)
			}
		}
		if c.expectCode != http.StatusOK {
			continue
		}

		// check expect values
		var body ResponsePull
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			t.Fatalf("#%d: failed to decord response, got err %v", i, err)
		}
		if got := len(body.Messages); got != c.expectSize {
			t.Errorf("#%d: message len want %d, got %d", i, c.expectSize, got)
		}

		// ack and sleep
		if c.isAck {
			ackIDs := make([]string, 0)
			for _, m := range body.Messages {
				ackIDs = append(ackIDs, m.AckID)
			}
			req := RequestAck{
				AckIDs: ackIDs,
			}

			var buf bytes.Buffer
			if err := json.NewEncoder(&buf).Encode(req); err != nil {
				t.Errorf("#%d: failed to encode json, got err %v", i, err)
			}
			client := dummyClient(t)
			_, err := client.Post(
				fmt.Sprintf("%s/subscription/%s/ack", ts.URL, "A"),
				"application/json", &buf)
			if err != nil {
				t.Fatalf("#%d: failed send ack request, got err %v", i, err)
			}
		}
		time.Sleep(c.sleep)
	}
}

func TestModifyAck(t *testing.T) {
	ts := setupServer(t)
	defer ts.Close()
	setupDummyTopics(t, ts)
	hackCreateShortAckSubscription(t)
	setupPublishMessages(t, ts, "a", PublishDatas{
		Messages: []PublishData{
			PublishData{Data: []byte(`test`), Attr: nil},
		},
	})

	decodePull := func(res *http.Response) ResponsePull {
		defer res.Body.Close()
		var buf ResponsePull
		if err := json.NewDecoder(res.Body).Decode(&buf); err != nil {
			t.Fatalf("failed to decode json, got err %v", err)
		}
		return buf
	}

	requestModifyAck := func(body RequestModifyAck) *http.Response {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("failed to encode json, got err %v", err)
		}
		client := dummyClient(t)
		res, err := client.Post(
			fmt.Sprintf("%s/subscription/%s/ack/modify", ts.URL, "A"),
			"application/json", &buf)
		if err != nil {
			t.Fatalf("failed to send request, got err %v", err)
		}
		return res
	}

	// pull and sleep hack short ack deadline
	r1 := decodePull(pullMessage(t, ts, "A", 1))
	time.Sleep(100 * time.Millisecond)

	// change ackID after sleep
	r2 := decodePull(pullMessage(t, ts, "A", 1))
	if r1.Messages[0].AckID == r2.Messages[0].AckID {
		t.Errorf("AckID want different, got AckID %s", r1.Messages[0].AckID)
	}

	// modify ack deadline
	// WARNING: need send request modify ack, until hack short ack deadline
	res := requestModifyAck(RequestModifyAck{
		AckIDs:             []string{r2.Messages[0].AckID},
		AckDeadlineSeconds: 1,
	})
	defer res.Body.Close()
	if got := res.StatusCode; got != http.StatusOK {
		t.Errorf("want status code %d, got %d", http.StatusOK, got)
	}

	// sleep hack short ack deadline, want no message
	time.Sleep(100 * time.Millisecond)
	if got := pullMessage(t, ts, "A", 1).StatusCode; got != http.StatusNotFound {
		t.Errorf("want status code %d, got %d", http.StatusNotFound, got)
	}

	// sleep modify ack deadline, want change AckID
	time.Sleep(1 * time.Second)
	r3 := decodePull(pullMessage(t, ts, "A", 1))
	if r2.Messages[0].AckID == r3.Messages[0].AckID {
		t.Errorf("AckID want different, got AckID %s", r2.Messages[0].AckID)
	}

	// unknown AckID, want error
	res = requestModifyAck(RequestModifyAck{
		AckIDs:             []string{r3.Messages[0].AckID, "unknown"},
		AckDeadlineSeconds: 1,
	})
	defer res.Body.Close()
	if got := res.StatusCode; got != http.StatusNotFound {
		t.Errorf("want status code %d, got %d", http.StatusNotFound, got)
	}
}
