package client

import "context"

// Topic is a accessor to a server topic
type Topic struct {
	ID string
	s  service
}

func newTopic(id string, s service) *Topic {
	return &Topic{
		ID: id,
		s:  s,
	}
}

// ByTopicID implements sort.Interface for the Topic.id
type ByTopicID []*Topic

func (a ByTopicID) Len() int           { return len(a) }
func (a ByTopicID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTopicID) Less(i, j int) bool { return a[i].ID < a[j].ID }

// PublishResult is a result for publish message
type PublishResult struct {
	done  chan struct{}
	msgID string
	err   error
}

// Get returns msgID and error
func (p *PublishResult) Get(ctx context.Context) (string, error) {
	// return result if already close done channel
	select {
	case <-p.done:
		return p.msgID, p.err
	default:
	}

	// waiting receive done channel and context channel
	select {
	case <-p.done:
		return p.msgID, p.err
	case <-ctx.Done():
		return "", p.err
	}
}

// Exists return whether the topic exists on the server.
func (t *Topic) Exists(ctx context.Context) (bool, error) {
	return t.s.topicExists(ctx, t.ID)
}

// Delete deletes the topic
func (t *Topic) Delete(ctx context.Context) error {
	return t.s.deleteTopic(ctx, t.ID)
}

// Subscriptions returns subscription list matched topic
func (t *Topic) Subscriptions(ctx context.Context) ([]*Subscription, error) {
	subIDs, err := t.s.listTopicSubscriptions(ctx, t.ID)
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
		done: make(chan struct{}),
	}

	go func() {
		msgID, err := t.s.publishMessages(ctx, t.ID, msg)
		pr.msgID = msgID
		pr.err = err
		close(pr.done)
	}()

	return pr
}

// StatsDetail returns stats detail of the Topic
func (t *Topic) StatsDetail(ctx context.Context) ([]byte, error) {
	return t.s.statsTopicDetail(ctx, t.ID)
}
