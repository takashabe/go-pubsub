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

func setupServer(t *testing.T) {
	s, err := NewServer("testdata/config.yaml")
	if err != nil {
		t.Fatalf("failed to NewServer, err=%v", err)
	}
	if err := s.InitDatastore(); err != nil {
		t.Fatalf("failed to InitDatastore, err=%v", err)
	}
}

func TestCreateAndGetTopic(t *testing.T) {
	setupServer(t)
	ts := httptest.NewServer(routes())
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
			[]byte("{\"name\":\"A\"}"),
			[]byte("{\"name\":\"A\"}"),
		},
		{
			"A",
			http.StatusNotFound, http.StatusOK,
			[]byte("{\"reason\":\"failed to create topic\"}"),
			[]byte("{\"name\":\"A\"}"),
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
