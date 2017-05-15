package models

import "net/url"

// Push represent push message in Subscription
type Push struct {
	Endpoint   *url.URL
	Attributes *Attributes
}

// Create Push object when valid URL
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
