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
	// handle topic
	createTopic(ctx context.Context, id string) error
	deleteTopic(ctx context.Context, id string) error
	topicExists(ctx context.Context, id string) (bool, error)
	listTopics(ctx context.Context) ([]string, error)
	listTopicSubscriptions(ctx context.Context, id string) ([]string, error)

	// handle subscription
	createSubscription(ctx context.Context, id string, cfg SubscriptionConfig) error
	getSubscriptionConfig(ctx context.Context, id string) (*SubscriptionConfig, error)
	listSubscriptions(ctx context.Context) ([]string, error)
	deleteSubscription(ctx context.Context, id string) error
	subscriptionExists(ctx context.Context, id string) (bool, error)
	modifyPushConfig(ctx context.Context, id string, cfg *PushConfig) error

	// handle message
	modifyAckDeadline(ctx context.Context, subID string, deadline time.Duration, ackIDs []string) error
	pullMessages(ctx context.Context, subID string, maxMessages int) ([]*Message, error)
	publishMessages(ctx context.Context, topicID string, msg *Message) (string, error)
	ack(ctx context.Context, subID string, ackIDs []string) error
}

// restService implemnet service interface for HTTP protocol
type restService struct {
	publisher  *restPublisher
	subscriber *restSubscriber
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

func (s *restService) createTopic(ctx context.Context, id string) error {
	res, err := s.publisher.sendRequest(ctx, "PUT", id, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusCreated, res)
}

func (s *restService) deleteTopic(ctx context.Context, id string) error {
	res, err := s.publisher.sendRequest(ctx, "DELETE", id, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusNoContent, res)
}

func (s *restService) topicExists(ctx context.Context, id string) (bool, error) {
	res, err := s.publisher.sendRequest(ctx, "GET", id, nil)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return http.StatusOK == res.StatusCode, nil
}

func (s *restService) listTopics(ctx context.Context) ([]string, error) {
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

func (s *restService) listTopicSubscriptions(ctx context.Context, id string) ([]string, error) {
	res, err := s.publisher.sendRequest(ctx, "GET", id+"/subscriptions", nil)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	type SubIDs struct {
		Subscription []string `json:"subscriptions"`
	}
	subs := SubIDs{}
	err = json.NewDecoder(res.Body).Decode(&subs)
	if err != nil {
		return nil, err
	}

	return subs.Subscription, nil
}

// ResourcePublishRequest represent the payload of the request Publish API
type ResourcePublishRequest struct {
	Messages []PublishMessage `json:"messages"`
}

// ResourcePublishResponse represent the payload of the response Publish API
type ResourcePublishResponse struct {
	MessageIDs []string `json:"message_ids"`
}

func (s *restService) publishMessages(ctx context.Context, id string, msg *Message) (string, error) {
	// TODO: acceptable the Message slice in args
	b := &ResourcePublishRequest{Messages: []PublishMessage{msg.toPublish()}}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(b)
	if err != nil {
		return "", err
	}
	res, err := s.publisher.sendRequest(ctx, "POST", id+"/publish", &buf)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	msgIDs := ResourcePublishResponse{}
	err = json.NewDecoder(res.Body).Decode(&msgIDs)
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

func (s *restService) createSubscription(ctx context.Context, id string, cfg SubscriptionConfig) error {
	if cfg.Topic == nil {
		return errors.New("require non-nil topic")
	}
	if !isValidAckDeadlineRange(cfg.AckTimeout) {
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

func (s *restService) getSubscriptionConfig(ctx context.Context, id string) (*SubscriptionConfig, error) {
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

func (s *restService) listSubscriptions(ctx context.Context) ([]string, error) {
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

func (s *restService) deleteSubscription(ctx context.Context, id string) error {
	res, err := s.subscriber.sendRequest(ctx, "DELETE", id, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusNoContent, res)
}

func (s *restService) subscriptionExists(ctx context.Context, id string) (bool, error) {
	res, err := s.subscriber.sendRequest(ctx, "GET", id, nil)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return http.StatusOK == res.StatusCode, nil
}

// ResourceModifyPush represent the payload of the ModifyPush API
type ResourceModifyPush struct {
	PushConfig *PushConfig `json:"push_config"`
}

func (s *restService) modifyPushConfig(ctx context.Context, id string, cfg *PushConfig) error {
	payload := &ResourceModifyPush{
		PushConfig: cfg,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(payload)
	if err != nil {
		return err
	}

	res, err := s.subscriber.sendRequest(ctx, "POST", id+"/push/modify", &buf)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusOK, res)
}

// ResourceModifyAck represent the payload of the ModifyAck API
type ResourceModifyAck struct {
	AckIDs             []string `json:"ack_ids"`
	AckDeadlineSeconds int64    `json:"ack_deadline_seconds"`
}

func (s *restService) modifyAckDeadline(ctx context.Context, subID string, deadline time.Duration, ackIDs []string) error {
	if !isValidAckDeadlineRange(deadline) {
		return errors.New("request error: invalid AckDeadline")
	}

	payload := &ResourceModifyAck{
		AckIDs:             ackIDs,
		AckDeadlineSeconds: int64(deadline.Seconds()),
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(payload)
	if err != nil {
		return err
	}

	res, err := s.subscriber.sendRequest(ctx, "POST", subID+"/ack/modify", &buf)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusOK, res)
}

// ResourcePullRequest represent the payload of the request Pull API
type ResourcePullRequest struct {
	MaxMessages int `json:"max_messages"`
}

// ResourcePullResponse represent the payload of the response Pull API
type ResourcePullResponse struct {
	Messages []struct {
		AckID   string   `json:"ack_id"`
		Message *Message `json:"message"`
	} `json:"receive_messages"`
}

func (s *restService) pullMessages(ctx context.Context, subID string, maxMessages int) ([]*Message, error) {
	if maxMessages <= 0 {
		maxMessages = 1
	}

	payload := &ResourcePullRequest{
		MaxMessages: maxMessages,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(payload)
	if err != nil {
		return nil, err
	}

	res, err := s.subscriber.sendRequest(ctx, "POST", subID+"/pull", &buf)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	rawMsgs := &ResourcePullResponse{}
	err = json.NewDecoder(res.Body).Decode(rawMsgs)
	if err != nil {
		return nil, err
	}
	msgs := []*Message{}
	for _, raw := range rawMsgs.Messages {
		raw.Message.AckID = raw.AckID
		msgs = append(msgs, raw.Message)
	}

	return msgs, nil
}

// ResourceAck represent the payload of the Ack API
type ResourceAck struct {
	AckIDs []string `json:"ack_ids"`
}

func (s *restService) ack(ctx context.Context, subID string, ackIDs []string) error {
	payload := &ResourceAck{
		AckIDs: ackIDs,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(payload)
	if err != nil {
		return err
	}

	res, err := s.subscriber.sendRequest(ctx, "POST", subID+"/ack", &buf)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return verifyHTTPStatusCode(http.StatusOK, res)
}

func verifyHTTPStatusCode(expect int, res *http.Response) error {
	if c := res.StatusCode; c != expect {
		return errors.Errorf("HTTP response error: expecte status code %d, but received %d", expect, c)
	}
	return nil
}

func isValidAckDeadlineRange(deadline time.Duration) bool {
	// TODO: AckDeadline needs to determine upper and lower limits
	return deadline > 0
}
