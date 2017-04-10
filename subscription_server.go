package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/takashabe/go-message-queue/models"
)

// SubscriptionServer is subscription frontend server
type SubscriptionServer struct{}

// ResourceSubscription represent create subscription request and response data
type ResourceSubscription struct {
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

func (s *SubscriptionServer) Get(w http.ResponseWriter, r *http.Request, id string) {
	sub, err := models.GetSubscription(id)
	if err != nil {
		Error(w, http.StatusNotFound, err, "not found subscription")
		return
	}
	Json(w, http.StatusOK, subscriptionToResource(sub))
}
