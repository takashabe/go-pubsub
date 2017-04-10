package main

import (
	"net/http"
	"net/http/httptest"
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
