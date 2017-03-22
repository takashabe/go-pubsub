package queue

import "time"

type Subscription struct {
	name        string
	messages    []Message
	ackTimeout  time.Duration
	pushClients []string
}

func (s *Subscription) Subscribe(m Message) {
}
