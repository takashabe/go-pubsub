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
	"github.com/takashabe/go-message-queue/datastore"
	"github.com/takashabe/go-message-queue/models"
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
	if env := os.Getenv("GO_MESSAGE_QUEUE_CONFIG"); len(env) != 0 {
		path = env
	} else {
		path = "testdata/config/memory.yaml"
	}
	s, err := NewServer(path)
	if err != nil {
		t.Fatalf("failed to NewServer, err=%v", err)
	}
	if err := s.InitDatastore(); err != nil {
		t.Fatalf("failed to InitDatastore, err=%v", err)
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
		f := fixture.NewFixture(a.Conn, "mysql")
		if err := f.LoadSQL("fixture/setup_mq_table.sql"); err != nil {
			t.Fatalf("failed to execute fixture, got err %v", err)
		}
	}

	// setup http server
	return httptest.NewServer(routes())
}

func setupDummyTopics(t *testing.T, ts *httptest.Server) {
	client := dummyClient(t)
	puts := []string{"a", "b", "c"}
	for i, p := range puts {
		req, err := http.NewRequest("PUT", ts.URL+"/topic/"+p, nil)
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
