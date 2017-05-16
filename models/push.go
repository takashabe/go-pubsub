package models

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
)

// Push is represent push message in Subscription
type Push struct {
	Endpoint   *url.URL
	Attributes *Attributes
}

// PushRequest is represent a send push http request
type PushRequest struct {
	Message        *Message `message`
	SubscriptionID string   `subscription`
}

// Create Push object
func NewPush(endpoint string, attributes map[string]string) (*Push, error) {
	if len(endpoint) == 0 {
		return &Push{
			Endpoint:   nil,
			Attributes: nil,
		}, nil
	}

	url, err := url.Parse(endpoint)
	if err != nil {
		return nil, ErrInvalidEndpoint
	}

	p := &Push{
		Endpoint: url,
		Attributes: &Attributes{
			Attr: make(map[string]string),
		},
	}
	for k, v := range attributes {
		p.Attributes.Set(k, v)
	}
	return p, nil
}

// HasValidEndpoint return bool push has valid endpoint
func (p *Push) HasValidEndpoint() bool {
	return p.Endpoint != nil
}

func (p *Push) sendMessage(msg *Message, subID string) error {
	body, err := json.Marshal(PushRequest{
		Message:        msg,
		SubscriptionID: subID,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("GET", p.Endpoint.String(), bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "failed to send push message")
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case 200, 201, 204, 102:
		return nil
	default:
		return errors.New("failed http status code")
	}
}
