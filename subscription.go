package queue

import "time"

type Subscription struct {
	name       string
	topic      *Topic
	messages   MessageList
	ackTimeout time.Duration
	push       *Push
}

// Create Subscription
func NewSubscription(name, topicName string, timeout int64, endpoint string, attr map[string]string) (*Subscription, error) {
	topic, err := GetTopic(topicName)
	if err != nil {
		return nil, err
	}
	s := &Subscription{
		name:  name,
		topic: topic,
	}
	s.SetAckTimeout(timeout)
	if err := s.SetPush(endpoint, attr); err != nil {
		return nil, err
	}

	return s, nil
}

// Receive Message from Topic
func (s *Subscription) Subscribe(m Message) {
}

// Succeed Message delivery. remove sent Message.
func (s *Subscription) Ack(ids ...string) {

}

// Set Ack timeout, arg time expect millisecond.
func (s *Subscription) SetAckTimeout(timeout int64) {
	if timeout < 0 {
		timeout = 0
	}
	s.ackTimeout = time.Duration(timeout) * time.Millisecond
}

// Set push endpoint with attributes, only one can be set as push endpoint.
func (s *Subscription) SetPush(endpoint string, attribute map[string]string) error {
	if len(endpoint) == 0 {
		return nil
	}

	p, err := NewPush(endpoint, attribute)
	if err != nil {
		return err
	}
	s.push = p
	return nil
}
