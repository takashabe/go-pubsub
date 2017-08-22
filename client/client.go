package client

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
)

// Message is a message sent to and received from the server
type Message struct {
	ID          string            `json:"message_id"`
	Data        []byte            `json:"data"`
	Attributes  map[string]string `json:"attributes"`
	AckID       string            `json:"-"`
	PublishTime time.Time         `json:"publish_time"`
}

// PublishMessage represent format of publish message
type PublishMessage struct {
	Data       []byte
	Attributes map[string]string
}

func (m *Message) toPublish() (io.Reader, error) {
	var buf bytes.Buffer
	p := &PublishMessage{
		Data:       m.Data,
		Attributes: m.Attributes,
	}
	err := json.NewEncoder(&buf).Encode(p)
	return &buf, err
}

// Client is a client for server
type Client struct {
	s service
}
