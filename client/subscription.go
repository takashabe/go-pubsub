package client

import (
	"context"
	"time"
)

// Subscription is a accessor to a server subscription
type Subscription struct {
	id string
	s  service
}

// SubscriptionConfig represent parameter of the Subscription
type SubscriptionConfig struct {
	Topic      *Topic
	PushConfig *PushConfig
	AckTimeout time.Duration
}

// PushConfig represent parameter of the push mode in Subscription
type PushConfig struct {
	Endpoint   string
	Attributes map[string]string
}

func newSubscription(id string, s service) *Subscription {
	return &Subscription{
		id: id,
		s:  s,
	}
}

// Config returns the current configuration for the Subscription
func (s *Subscription) Config(ctx context.Context) (*SubscriptionConfig, error) {
	return s.s.getSubscriptionConfig(ctx, s.id)
}

// Delete deletes the Subscription
func (s *Subscription) Delete(ctx context.Context) error {
	return s.s.deleteSubscription(ctx, s.id)
}
