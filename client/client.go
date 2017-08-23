package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Message is a message sent to and received from the server
type Message struct {
	ID          string            `json:"message_id"`
	Data        []byte            `json:"data"`
	Attributes  map[string]string `json:"attributes"`
	AckID       string            `json:"-"`
	PublishTime time.Time         `json:"publish_time"`
}

// PublishMessage represent format of publish message
type PublishMessage struct {
	Data       []byte
	Attributes map[string]string
}

func (m *Message) toPublish() (io.Reader, error) {
	var buf bytes.Buffer
	p := &PublishMessage{
		Data:       m.Data,
		Attributes: m.Attributes,
	}
	err := json.NewEncoder(&buf).Encode(p)
	return &buf, err
}

// Client is a client for server
type Client struct {
	s service
}

// NewClient returns a new pubsub client
func NewClient(ctx context.Context, addr string) (*Client, error) {
	if _, err := url.Parse(addr); err != nil {
		return nil, err
	}

	// TODO: enable to designate any client
	httpClient := http.Client{}
	return &Client{
		s: &restService{
			publisher: &restPublisher{
				serverURL:  addr,
				httpClient: httpClient,
			},
			subscriber: &restSubscriber{
				serverURL:  addr,
				httpClient: httpClient,
			},
		},
	}, nil
}

// CreateTopic creates new Topic
func (c *Client) CreateTopic(ctx context.Context, id string) (*Topic, error) {
	err := c.s.createTopic(ctx, id)
	if err != nil {
		return nil, err
	}

	return newTopic(id, c.s), nil
}

// CreateSubscription creates new Subscription
func (c *Client) CreateSubscription(ctx context.Context, id string, cfg SubscriptionConfig) (*Subscription, error) {
	err := c.s.createSubscription(ctx, id, cfg)
	if err != nil {
		return nil, err
	}

	return newSubscription(id, c.s), nil
}
