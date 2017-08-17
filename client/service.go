package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// service is an accessor to server API used by this package
type service interface {
	createTopic(ctx context.Context, id string) error
	deleteTopic(ctx context.Context, id string) error
	topicExists(ctx context.Context, id string) (bool, error)
	listTopics(ctx context.Context) ([]string, error)
	listTopicSubscriptions(ctx context.Context, id string) ([]string, error)

	// TODO: implements
	createSubscription(ctx context.Context, id string, cfg SubscriptionConfig) error
	getSubscriptionConfig(ctx context.Context, id string) (*SubscriptionConfig, error)
	listSubscriptions(ctx context.Context) ([]string, error)
	deleteSubscription(ctx context.Context, id string) error
	// subscriptionExists(ctx context.Context, id string) (bool, error)
	// modifyPushConfig(ctx context.Context, id string, cfg PushConfig) error

	// modifyAckDeadline(ctx context.Context, subID string, deadline time.Duration, ackIDs []string) error
	// pullMessages(ctx context.Context, subID string, maxMessages int) ([]*Message, error)
	publishMessages(ctx context.Context, topicID string, msg *Message) (string, error)
	//
	// ack(ctx context.Context, subID string, ackIDs []string) error

	// close() error
}

// httpService implemnet service interface for HTTP protocol
type httpService struct {
	publisher  restPublisher
	subscriber restSubscriber
}

type restPublisher struct {
	serverURL  string
	httpClient http.Client
}

type restSubscriber struct {
	serverURL  string
	httpClient http.Client
}

func (p *restPublisher) sendRequest(ctx context.Context, method, url string, body io.Reader) (*http.Response, error) {
	return sendRequest(ctx, p.httpClient, method, p.serverURL+url, body)
}

func (s *restSubscriber) sendRequest(ctx context.Context, method, url string, body io.Reader) (*http.Response, error) {
	return sendRequest(ctx, s.httpClient, method, s.serverURL+url, body)
}

func sendRequest(ctx context.Context, client http.Client, method, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	return client.Do(req)
}

func (s *httpService) createTopic(ctx context.Context, id string) error {
	res, err := s.publisher.sendRequest(ctx, "PUT", id, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusCreated, res)
}

func (s *httpService) deleteTopic(ctx context.Context, id string) error {
	res, err := s.publisher.sendRequest(ctx, "DELETE", id, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusNoContent, res)
}

func (s *httpService) topicExists(ctx context.Context, id string) (bool, error) {
	res, err := s.publisher.sendRequest(ctx, "GET", id, nil)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return http.StatusOK == res.StatusCode, nil
}

func (s *httpService) listTopics(ctx context.Context) ([]string, error) {
	res, err := s.publisher.sendRequest(ctx, "GET", "", nil)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	type topicID struct {
		name string
	}
	ids := []topicID{}
	err = json.NewDecoder(res.Body).Decode(ids)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, v := range ids {
		ret = append(ret, v.name)
	}
	return ret, nil
}

func (s *httpService) listTopicSubscriptions(ctx context.Context, id string) ([]string, error) {
	res, err := s.publisher.sendRequest(ctx, "GET", id+"/subscriptions", nil)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	type subIDs struct {
		subscription []string
	}
	subs := subIDs{}
	err = json.NewDecoder(res.Body).Decode(subs)
	if err != nil {
		return nil, err
	}

	return subs.subscription, nil
}

func (s *httpService) publishMessages(ctx context.Context, id string, msg *Message) (string, error) {
	b, err := msg.toPublish()
	if err != nil {
		return "", err
	}
	res, err := s.publisher.sendRequest(ctx, "GET", id+"/publish", b)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	type ResponsePublish struct {
		MessageIDs []string `json:"messageIDs"`
	}
	msgIDs := ResponsePublish{}
	err = json.NewDecoder(res.Body).Decode(msgIDs)
	if err != nil {
		return "", err
	}

	// NOTE: expect sent a one message only
	return msgIDs.MessageIDs[0], nil
}

// ResourceSusbscription represent body of request/response the Subscription parameter
type ResourceSusbscription struct {
	Name       string      `json:"name"`
	Topic      string      `json:"topic"`
	PushConfig *PushConfig `json:"push_config"`
	AckTimeout int64       `json:"ack_deadline_seconds"`
}

func (s *httpService) createSubscription(ctx context.Context, id string, cfg SubscriptionConfig) error {
	// TODO: AckTimeout needs to determine upper and lower limits
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = 10 * time.Second
	}

	rs := &ResourceSusbscription{
		Name:       id,
		Topic:      cfg.Topic.id,
		PushConfig: cfg.PushConfig,
		AckTimeout: int64(cfg.AckTimeout.Seconds()),
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(rs)
	if err != nil {
		return err
	}

	res, err := s.subscriber.sendRequest(ctx, "PUT", id, &buf)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusCreated, res)
}

func (s *httpService) getSubscriptionConfig(ctx context.Context, id string) (*SubscriptionConfig, error) {
	res, err := s.subscriber.sendRequest(ctx, "GET", id, nil)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	rs := &ResourceSusbscription{}
	err = json.NewDecoder(res.Body).Decode(rs)
	if err != nil {
		return nil, err
	}
	cfg := &SubscriptionConfig{
		Topic:      newTopic(rs.Topic, s),
		PushConfig: rs.PushConfig,
		AckTimeout: time.Duration(rs.AckTimeout),
	}

	return cfg, nil
}

func (s *httpService) listSubscriptions(ctx context.Context) ([]string, error) {
	res, err := s.subscriber.sendRequest(ctx, "GET", "", nil)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	subs := []ResourceSusbscription{}
	err = json.NewDecoder(res.Body).Decode(subs)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, v := range subs {
		ret = append(ret, v.Name)
	}
	return ret, nil
}

func (s *httpService) deleteSubscription(ctx context.Context, id string) error {
	res, err := s.subscriber.sendRequest(ctx, "DELETE", id, nil)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusNoContent, res)
}

func verifyHTTPStatusCode(expect int, res *http.Response) error {
	if c := res.StatusCode; c != expect {
		return errors.Errorf("HTTP response error: expecte status code %d, but received %d", expect, c)
	}
	return nil
}
