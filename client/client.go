package client

import (
	"context"
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
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

func (m *Message) toPublish() PublishMessage {
	return PublishMessage{
		Data:       m.Data,
		Attributes: m.Attributes,
	}
}

// Client is a client for server
type Client struct {
	s service
}

// NewClient returns a new pubsub client
func NewClient(ctx context.Context, addr string) (*Client, error) {
	if addr[len(addr)-1] != '/' {
		addr = addr + "/"
	}
	if _, err := url.Parse(addr); err != nil {
		return nil, err
	}

	// TODO: enable to designate any client
	httpClient := http.Client{}
	return &Client{
		s: &restService{
			publisher: &restPublisher{
				serverURL:  addr + "topic/",
				httpClient: httpClient,
			},
			subscriber: &restSubscriber{
				serverURL:  addr + "subscription/",
				httpClient: httpClient,
			},
			monitoring: &restMonitoring{
				serverURL:  addr + "stats/",
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

// Topic returns reference of the topic
func (c *Client) Topic(id string) *Topic {
	return newTopic(id, c.s)
}

// Topics returns existing the topic list
func (c *Client) Topics(ctx context.Context) ([]*Topic, error) {
	ids, err := c.s.listTopics(ctx)
	if err != nil {
		return nil, err
	}
	topics := []*Topic{}
	for _, id := range ids {
		topics = append(topics, newTopic(id, c.s))
	}
	return topics, nil
}

// CreateSubscription creates new Subscription
func (c *Client) CreateSubscription(ctx context.Context, id string, cfg SubscriptionConfig) (*Subscription, error) {
	err := c.s.createSubscription(ctx, id, cfg)
	if err != nil {
		return nil, err
	}

	return newSubscription(id, c.s), nil
}

// Subscription returns reference of the subscription
func (c *Client) Subscription(id string) *Subscription {
	return newSubscription(id, c.s)
}

// Subscriptions returns all existing the subscription list
func (c *Client) Subscriptions(ctx context.Context) ([]*Subscription, error) {
	ids, err := c.s.listSubscriptions(ctx)
	if err != nil {
		return nil, err
	}
	subscriptions := []*Subscription{}
	for _, id := range ids {
		subscriptions = append(subscriptions, newSubscription(id, c.s))
	}
	return subscriptions, nil
}

// Stats returns stats summary
func (c *Client) Stats(ctx context.Context) ([]byte, error) {
	return c.s.statsSummary(ctx)
}
