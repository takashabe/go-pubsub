package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/takashabe/go-message-queue/models"
)

// SubscriptionServer is subscription frontend server
type SubscriptionServer struct{}

// ResourceSubscription represent create subscription request and response data
type ResourceSubscription struct {
	Name       string     `json:"name"`
	Topic      string     `json:"topic"`
	Push       PushConfig `json:"push_config"`
	AckTimeout int64      `json:"ack_deadline_seconds"`
}
type PushConfig struct {
	Endpoint string            `json:"endpoint"`
	Attr     map[string]string `json:"attributes"`
}

// subscriptionToResource is Subscription object convert to ResourceSubscription
func subscriptionToResource(s *models.Subscription) ResourceSubscription {
	pushConfig := PushConfig{}
	if s.Push != nil {
		pushConfig.Endpoint = s.Push.Endpoint.String()
		pushConfig.Attr = s.Push.Attributes.Dump()
	}

	return ResourceSubscription{
		Name:       s.Name,
		Topic:      s.Topic.Name,
		Push:       pushConfig,
		AckTimeout: int64(s.AckTimeout / time.Second),
	}
}

// Create is create subscription
func (s *SubscriptionServer) Create(w http.ResponseWriter, r *http.Request, id string) {
	// parse request
	var req ResourceSubscription
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		Error(w, http.StatusNotFound, err, "failed to parsed request")
		return
	}

	// create subscription
	sub, err := models.NewSubscription(id, req.Topic, req.AckTimeout, req.Push.Endpoint, req.Push.Attr)
	if err != nil {
		Error(w, http.StatusNotFound, err, "failed to create subscription")
		return
	}
	Json(w, http.StatusCreated, subscriptionToResource(sub))
}

// Get is get already exist subscription
func (s *SubscriptionServer) Get(w http.ResponseWriter, r *http.Request, id string) {
	sub, err := models.GetSubscription(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "not found subscription")
		return
	}
	Json(w, http.StatusOK, subscriptionToResource(sub))
}

// List is gets subscription list
func (s *SubscriptionServer) List(w http.ResponseWriter, r *http.Request) {
	subs, err := models.ListSubscription()
	if err != nil {
		Error(w, http.StatusNotFound, err, "not found subscription")
		return
	}
	sort.Sort(models.BySubscriptionName(subs))
	resourceSubs := make([]ResourceSubscription, 0)
	for _, sub := range subs {
		resourceSubs = append(resourceSubs, subscriptionToResource(sub))
	}
	Json(w, http.StatusOK, resourceSubs)
}

// RequestPull is represents request json for Pull
type RequestPull struct {
	// TODO: ReturnImmediately bool
	MaxMessages int
}

// ResponsePull is represents response json for Pull
type ResponsePull struct {
	AckMessages []ResourceAckMessage `json:"receive_messages"`
}

// represent response ack id and message json
type ResourceAckMessage struct {
	AckID   string          `json:"ack_id"`
	Message ResourceMessage `json:"message"`
}
type ResourceMessage struct {
	ID          string            `json:"message_id"`
	Data        []byte            `json:"data"`
	Attributes  map[string]string `json:"attributes"`
	PublishedAt time.Time         `json:"published_at"`
}

// messageToResource is Message object convert to ResourceMessage
func messageToResource(m *models.Message) ResourceAckMessage {
	msg := ResourceMessage{
		ID:          m.ID,
		Data:        m.Data,
		Attributes:  m.Attributes.Dump(),
		PublishedAt: m.PublishedAt,
	}
	return ResourceAckMessage{
		AckID:   m.ID,
		Message: msg,
	}
}

// Pull is get some messages
func (s *SubscriptionServer) Pull(w http.ResponseWriter, r *http.Request, id string) {
	// TODO: response timing flag, "immediately" and "wait untile at least one message"
	// parse request
	var req RequestPull
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		Error(w, http.StatusNotFound, err, "failed to parsed request")
		return
	}

	// pull messages
	sub, err := models.GetSubscription(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "not found subscription")
		return
	}
	msgs, err := sub.Pull(req.MaxMessages)
	if err != nil {
		Error(w, http.StatusNotFound, err, "not found message")
		return
	}
	res := ResponsePull{
		AckMessages: make([]ResourceAckMessage, 0, len(msgs)),
	}
	for _, m := range msgs {
		res.AckMessages = append(res.AckMessages, messageToResource(m))
	}
	Json(w, http.StatusOK, res)
}

// Delete is delete subscription
func (s *SubscriptionServer) Delete(w http.ResponseWriter, r *http.Request, id string) {
	sub, err := models.GetSubscription(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "subscription already not exist")
		return
	}
	if err := sub.Delete(); err != nil {
		Error(w, http.StatusInternalServerError, err, "failed to delete subscription")
		return
	}
	Json(w, http.StatusNoContent, "")
}
