package main

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func dummyClient(t *testing.T) *http.Client {
	// suppression to redirect
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func setupServer(t *testing.T) *httptest.Server {
	// setup datastore
	s, err := NewServer("testdata/config.yaml")
	if err != nil {
		t.Fatalf("failed to NewServer, err=%v", err)
	}
	if err := s.InitDatastore(); err != nil {
		t.Fatalf("failed to InitDatastore, err=%v", err)
	}

	// setup http server
	return httptest.NewServer(routes())
}

func setupDummyTopics(t *testing.T, ts *httptest.Server) {
	client := dummyClient(t)
	puts := []string{"a", "b", "c"}
	for i, p := range puts {
		req, err := http.NewRequest("PUT", ts.URL+"/topic/create/"+p, nil)
		if err != nil {
			t.Fatalf("#%d: failed to create request", i)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatalf("#%d: failed to send request", i)
		}
		defer res.Body.Close()
	}
}

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
	want = []byte(`[{"name":"a"},{"name":"b"}]`)
	if got, _ := ioutil.ReadAll(res.Body); !reflect.DeepEqual(got, want) {
		t.Errorf("want %s, got %s", want, got)
	}
}
