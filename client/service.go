package client

import (
	"context"
	"encoding/json"
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
	topicURL    string
	topicClient http.Client

	subscriptionURL    string
	subscriptionClient http.Client
}

func (s *httpService) createTopic(ctx context.Context, id string) error {
	req, err := http.NewRequest("PUT", s.topicURL+"/"+id, nil)
	if err != nil {
		return err
	}
	res, err := s.topicClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}

func (s *httpService) deleteTopic(ctx context.Context, id string) error {
	req, err := http.NewRequest("DELETE", s.topicURL+"/"+id, nil)
	if err != nil {
		return err
	}
	res, err := s.topicClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}

func (s *httpService) topicExists(ctx context.Context, id string) (bool, error) {
	req, err := http.NewRequest("GET", s.topicURL+"/"+id, nil)
	if err != nil {
		return false, err
	}
	res, err := s.topicClient.Do(req)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return http.StatusOK == res.StatusCode, nil
}

func (s *httpService) listTopics(ctx context.Context) ([]string, error) {
	req, err := http.NewRequest("GET", s.topicURL, nil)
	if err != nil {
		return nil, err
	}
	res, err := s.topicClient.Do(req)
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
	req, err := http.NewRequest("GET", s.topicURL+"/"+id+"/subscriptions", nil)
	if err != nil {
		return nil, err
	}
	res, err := s.topicClient.Do(req)
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
