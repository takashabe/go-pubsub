package server

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
	if s.PushConfig != nil && s.PushConfig.HasValidEndpoint() {
		pushConfig.Endpoint = s.PushConfig.Endpoint.String()
		pushConfig.Attr = s.PushConfig.Attributes.Dump()
	}

	return ResourceSubscription{
		Name:       s.Name,
		Topic:      s.TopicID,
		Push:       pushConfig,
		AckTimeout: int64(s.DefaultAckDeadline / time.Second),
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
	Messages []*models.PullMessage `json:"receive_messages"`
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
	Json(w, http.StatusOK, ResponsePull{Messages: msgs})
}

// RequestAck represent request ack API json
type RequestAck struct {
	AckIDs []string `json:"ack_ids"`
}

// Ack is setting ack state
func (s *SubscriptionServer) Ack(w http.ResponseWriter, r *http.Request, id string) {
	// parse request
	var req RequestAck
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		Error(w, http.StatusNotFound, err, "failed to parsed request")
		return
	}

	// ack message
	sub, err := models.GetSubscription(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "not found subscription")
		return
	}
	if err := sub.Ack(req.AckIDs...); err != nil {
		Error(w, http.StatusNotFound, err, "failed to ack message")
		return
	}
	Json(w, http.StatusOK, "")
}

// RequestModifyAck represent request ModifyAck API json
type RequestModifyAck struct {
	AckIDs             []string `json:"ack_ids"`
	AckDeadlineSeconds int64    `json:"ack_deadline_seconds"`
}

// ModifyAck is ack timeout setting already delivered message
func (s *SubscriptionServer) ModifyAck(w http.ResponseWriter, r *http.Request, id string) {
	// parse request
	var req RequestModifyAck
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		Error(w, http.StatusNotFound, err, "failed to parsed request")
		return
	}

	// modify ack
	sub, err := models.GetSubscription(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "not found subscription")
		return
	}
	for _, ackID := range req.AckIDs {
		if err := sub.ModifyAckDeadline(ackID, req.AckDeadlineSeconds); err != nil {
			Error(w, http.StatusNotFound, err, "failed to modify ack deadline seconds")
			return
		}
	}
	Json(w, http.StatusOK, "")
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
