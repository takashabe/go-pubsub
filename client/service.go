package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// service is an accessor to server API used by this package
type service interface {
	createTopic(ctx context.Context, id string) error
	deleteTopic(ctx context.Context, id string) error
	topicExists(ctx context.Context, id string) (bool, error)
	listTopics(ctx context.Context) ([]string, error)
	listTopicSubscriptions(ctx context.Context, id string) ([]string, error)

	// TODO: implements
	// createSubscription(ctx context.Context, id string, cfg SubscriptionConfig) error
	// getSubscriptionConfig(ctx context.Context, id string) (SubscriptionConfig, string, error)
	// listSubscriptions(ctx context.Context) []string
	// deleteSubscription(ctx context.Context, id string) error
	// subscriptionExists(ctx context.Context, id string) (bool, error)
	// modifyPushConfig(ctx context.Context, id string, cfg PushConfig) error

	// modifyAckDeadline(ctx context.Context, subID string, deadline time.Duration, ackIDs []string) error
	// pullMessages(ctx context.Context, subID string, maxMessages int) ([]*Message, error)
	publishMessages(ctx context.Context, topicID string, msg *Message) (string, error)
	//
	// ack(ctx context.Context, subID string, ackIDs []string) error

	close() error
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
	req, err := http.NewRequest(method, p.serverURL+url, body)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	return p.httpClient.Do(req)
}

func (s *httpService) createTopic(ctx context.Context, id string) error {
	res, err := s.publisher.sendRequest("PUT", id, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}

func (s *httpService) deleteTopic(ctx context.Context, id string) error {
	res, err := s.publisher.sendRequest("DELETE", id, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}

func (s *httpService) topicExists(ctx context.Context, id string) (bool, error) {
	rse, err := s.publisher.sendRequest("GET", id, nil)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return http.StatusOK == res.StatusCode, nil
}

func (s *httpService) listTopics(ctx context.Context) ([]string, error) {
	res, err := s.publisher.sendRequest("GET", "", nil)
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
	res, err := s.publisher.sendRequest("GET", id+"/subscriptions", nil)
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
	req, err := s.publisher.sendRequest("GET", id+"/publish", b)
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
