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
