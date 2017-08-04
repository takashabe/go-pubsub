package client

import "context"

// Topic is a accessor to a server topic
type Topic struct {
	id string
}

// Exists return whether the topic exists on the server.
func (t *Topic) Exists() (bool, error) {
	return false, nil
}

// Subscriptions returns subscription list matched topic
func (t *Topic) Subscriptions(ctx context.Context) ([]Subscription, error) {
	return nil, nil
}
