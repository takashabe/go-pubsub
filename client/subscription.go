package client

// Subscription is a accessor to a server subscription
type Subscription struct {
	id string
	s  service
}

func newSubscription(id string, s service) *Subscription {
	return &Subscription{
		id: id,
		s:  s,
	}
}
