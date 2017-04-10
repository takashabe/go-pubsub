package models

import "net/url"

// Push represent push message in Subscription
type Push struct {
	Endpoint   *url.URL    `json:"endpoint"`
	Attributes *Attributes `json:"attributes"`
}

// Create Push object when valid URL
func NewPush(endpoint string, attributes map[string]string) (*Push, error) {
	url, err := url.Parse(endpoint)
	if err != nil {
		return nil, ErrInvalidEndpoint
	}

	p := &Push{
		Endpoint: url,
		Attributes: &Attributes{
			attr: make(map[string]string),
		},
	}
	for k, v := range attributes {
		p.Attributes.set(k, v)
	}
	return p, nil
}
