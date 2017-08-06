package client

import "context"

// Topic is a accessor to a server topic
type Topic struct {
	id string
	s  service
}

// PublishResult is a result for publish message
type PublishResult struct {
	ready chan struct{}
	err   error
}

// Exists return whether the topic exists on the server.
func (t *Topic) Exists(ctx context.Context) (bool, error) {
	return t.s.topicExists(ctx, t.id)
}

// Delete deletes the topic
func (t *Topic) Delete(ctx context.Context) error {
	return t.s.deleteTopic(ctx, t.id)
}

// Subscriptions returns subscription list matched topic
func (t *Topic) Subscriptions(ctx context.Context) ([]*Subscription, error) {
	subIDs, err := t.s.listTopicSubscriptions(ctx, t.id)
	if err != nil {
		return nil, err
	}

	subs := []*Subscription{}
	for _, id := range subIDs {
		subs = append(subs, newSubscription(id, t.s))
	}
	return subs, nil
}

// Publish asynchronously send message, and return immediate PublishResult
func (t *Topic) Publish(ctx context.Context, msg *Message) *PublishResult {
	pr := &PublishResult{
		ready: make(chan struct{}),
	}

	// TODO: update PublishResult in another asynchronously function
	t.s.publishMessages(ctx, t.id, msg)

	return pr
}
