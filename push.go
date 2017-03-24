package queue

import (
	"errors"
	"net/url"
)

// Errors about push
var (
	ErrInvalidEndpoint = errors.New("invalid endpoint URL format")
)

// Push represent push message in Subscription
type Push struct {
	endpoint   *url.URL
	attributes *Attributes
}

// Create Push object when valid URL
func NewPush(endpoint string, attributes map[string]string) (*Push, error) {
	url, err := url.Parse(endpoint)
	if err != nil {
		return nil, ErrInvalidEndpoint
	}

	p := &Push{
		endpoint: url,
		attributes: &Attributes{
			attr: make(map[string]string),
		},
	}
	for k, v := range attributes {
		p.attributes.set(k, v)
	}
	return p, nil
}
