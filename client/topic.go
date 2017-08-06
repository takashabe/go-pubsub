package client

import "context"

// Topic is a accessor to a server topic
type Topic struct {
	id string
	s  service
}

// Exists return whether the topic exists on the server.
	return false, nil
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
