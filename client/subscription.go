package client

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

// Subscription is a accessor to a server subscription
type Subscription struct {
	ID string
	s  service
}

// SubscriptionConfig represent parameter of the Subscription
type SubscriptionConfig struct {
	Topic      *Topic
	PushConfig *PushConfig
	AckTimeout time.Duration
}

// SubscriptionConfigToUpdate is updatable parameter for the existed Subscription
type SubscriptionConfigToUpdate struct {
	PushConfig *PushConfig
}

// PushConfig represent parameter of the push mode in Subscription
type PushConfig struct {
	Endpoint   string
	Attributes map[string]string
}

func newSubscription(id string, s service) *Subscription {
	return &Subscription{
		ID: id,
		s:  s,
	}
}

// BySubscriptionID implements sort.Interface for the Subscription.id
type BySubscriptionID []*Subscription

func (a BySubscriptionID) Len() int           { return len(a) }
func (a BySubscriptionID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BySubscriptionID) Less(i, j int) bool { return a[i].ID < a[j].ID }

// Exists return whether the subscription exists on the server.
func (s *Subscription) Exists(ctx context.Context) (bool, error) {
	return s.s.subscriptionExists(ctx, s.ID)
}

// Config returns the current configuration for the Subscription
func (s *Subscription) Config(ctx context.Context) (*SubscriptionConfig, error) {
	return s.s.getSubscriptionConfig(ctx, s.ID)
}

// Delete deletes the Subscription
func (s *Subscription) Delete(ctx context.Context) error {
	return s.s.deleteSubscription(ctx, s.ID)
}

// Receive calls fn for the fetched messages from the Subscription.
// send a nack requests when an error occurs via the Pull API.
func (s *Subscription) Receive(ctx context.Context, fn func(ctx context.Context, msg *Message)) error {
	// TODO: number of receive message extract to ReceiveConfig
	msgs, err := s.s.pullMessages(ctx, s.ID, 1)
	if err != nil {
		// send nack request to the pulled messages
		if msgs != nil {
			ackIDs := []string{}
			for _, msg := range msgs {
				ackIDs = append(ackIDs, msg.AckID)
			}
			if nackErr := s.Nack(ctx, ackIDs); nackErr != nil {
				errors.Wrapf(err, "failed to nack messages: %s", nackErr.Error())
			}
		}
		return err
	}

	for _, msg := range msgs {
		fn(ctx, msg)
	}
	return nil
}

// Ack calls Ack API for the ackIDs
func (s *Subscription) Ack(ctx context.Context, ackIDs []string) error {
	return s.s.ack(ctx, s.ID, ackIDs)
}

// Nack releases messages from the Subscription.
// As a result, another subscriber can pull message.
func (s *Subscription) Nack(ctx context.Context, ackIDs []string) error {
	// nack is represented by setting AckDeadline to zero
	return s.s.modifyAckDeadline(ctx, s.ID, 0, ackIDs)
}

// Update updates an existing Subscription
func (s *Subscription) Update(ctx context.Context, cfg *SubscriptionConfigToUpdate) error {
	return s.s.modifyPushConfig(ctx, s.ID, cfg.PushConfig)
}

// StatsDetail returns stats detail of the Subscription
func (s *Subscription) StatsDetail(ctx context.Context) ([]byte, error) {
	return s.s.statsSubscriptionDetail(ctx, s.ID)
}
