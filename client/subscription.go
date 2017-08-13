package client

import "time"

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
