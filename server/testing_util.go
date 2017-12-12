package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	fixture "github.com/takashabe/go-fixture"
	_ "github.com/takashabe/go-fixture/mysql" // mysql driver
	"github.com/takashabe/go-pubsub/datastore"
	"github.com/takashabe/go-pubsub/models"
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
	var path string
	if env := os.Getenv("GO_PUBSUB_CONFIG"); len(env) != 0 {
		path = env
	} else {
		path = "testdata/config/memory.yaml"
	}
	s, err := NewServer(path)
	if err != nil {
		t.Fatalf("failed to NewServer, err=%v", err)
	}
	if err := s.PrepareServer(); err != nil {
		t.Fatalf("failed to PrepareServer, err=%v", err)
	}

	// flush datastore. only Redis
	d, err := datastore.LoadDatastore(s.cfg.Datastore)
	if err != nil {
		t.Fatalf("failed to load datastore, got err %v", err)
	}
	switch a := d.(type) {
	case *datastore.Redis:
		conn := a.Pool.Get()
		defer conn.Close()

		_, err := conn.Do("FLUSHDB")
		if err != nil {
			t.Fatalf("failed to FLUSHDB on Redis, got error %v", err)
		}
	case *datastore.MySQL:
		f, err := fixture.NewFixture(a.Conn, "mysql")
		if err != nil {
			t.Fatalf("failed to initialize fixture, got err %v", err)
		}
		if err := f.LoadSQL("fixture/setup_table.sql"); err != nil {
			t.Fatalf("failed to execute fixture, got err %v", err)
		}
	}

	// setup http server
	return httptest.NewServer(Routes())
}

func setupDummyTopics(t *testing.T, ts *httptest.Server) {
	topics := []string{"a", "b", "c"}
	for _, id := range topics {
		createDummyTopic(t, ts, id)
	}
}

func createDummyTopic(t *testing.T, ts *httptest.Server, id string) {
	client := dummyClient(t)
	req, err := http.NewRequest("PUT", ts.URL+"/topic/"+id, nil)
	if err != nil {
		t.Fatal("failed to create request")
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal("failed to send request")
	}
	defer res.Body.Close()
}

func createDummySubscription(t *testing.T, ts *httptest.Server, resource ResourceSubscription) {
	client := dummyClient(t)
	b, err := json.Marshal(resource)
	if err != nil {
		t.Fatal("failed to encode json")
	}
	req, err := http.NewRequest("PUT",
		fmt.Sprintf("%s/subscription/%s", ts.URL, resource.Name), bytes.NewBuffer(b))
	if err != nil {
		t.Fatal("failed to create request")
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal("failed to send request")
	}
	defer res.Body.Close()
}

func setupDummyTopicAndSub(t *testing.T, ts *httptest.Server) {
	setupDummyTopics(t, ts)
	reqs := []ResourceSubscription{
		ResourceSubscription{
			Name:       "A",
			Topic:      "a",
			AckTimeout: 10,
		},
		ResourceSubscription{
			Name:       "B",
			Topic:      "a",
			AckTimeout: 10,
		},
	}
	for _, r := range reqs {
		createDummySubscription(t, ts, r)
	}
}

func setupPublishMessages(t *testing.T, ts *httptest.Server, topicName string, pub PublishDatas) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(pub); err != nil {
		t.Fatalf("failed to encode PublishData")
	}
	client := dummyClient(t)
	_, err := client.Post(fmt.Sprintf("%s/topic/%s/publish", ts.URL, topicName), "application/json", &buf)
	if err != nil {
		t.Fatal("failed to send request")
	}
}

func dummyPublishMessage(t *testing.T, ts *httptest.Server) {
	setupPublishMessages(t, ts, "a", PublishDatas{
		Messages: []PublishData{
			PublishData{Data: []byte(`test1`), Attr: nil},
			PublishData{Data: []byte(`test2`), Attr: map[string]string{"1": "2"}},
			PublishData{Data: []byte(`test3`), Attr: map[string]string{"2": "3"}},
		},
	})
}

// warning: direct access to models package
func hackCreateShortAckSubscription(t *testing.T) {
	// require created topic "a"
	s, err := models.NewSubscription("A", "a", 0, "", nil)
	if err != nil {
		t.Fatalf("failed to create subscription, got err %v", err)
	}
	s.DefaultAckDeadline = 100 * time.Millisecond
	if err := s.Save(); err != nil {
		t.Fatalf("failed to save subscription, got err %v", err)
	}
}

func pullMessage(t *testing.T, ts *httptest.Server, sub string, size int) *http.Response {
	reqData := RequestPull{
		MaxMessages: size,
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(reqData); err != nil {
		t.Fatal("failed to encode struct")
	}
	client := dummyClient(t)
	res, err := client.Post(
		fmt.Sprintf("%s/subscription/%s/pull", ts.URL, sub),
		"application/json", &buf)
	if err != nil {
		t.Fatalf("failed to send request, got err %v", err)
	}
	return res
}
