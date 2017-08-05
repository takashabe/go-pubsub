package client

import (
	"time"
)

// Message is a message sent to and received from the server
type Message struct {
	ID          string
	Data        []byte
	Attributes  map[string]string
	AckID       string
	PublishTime time.Time
}

// Client is a client for server
type Client struct {
	s service
}
