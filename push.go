package queue

import (
	"errors"
	"net/url"
	"sync"
)

// Errors about push
var (
	ErrInvalidEndpoint = errors.New("invalid endpoint URL format")
)

type pushAttributes struct {
	attr map[string]string
	mu   sync.Mutex
}

func (a *pushAttributes) set(key, value string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.attr[key] = value
}

func (a *pushAttributes) get(key string) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.attr[key]
	return v, ok
}

// Push represent push message in Subscription
type Push struct {
	endpoint   *url.URL
	attributes *pushAttributes
}

// Create Push object when valid URL
func NewPush(endpoint string, attributes map[string]string) (*Push, error) {
	url, err := url.Parse(endpoint)
	if err != nil {
		return nil, ErrInvalidEndpoint
	}

	p := &Push{
		endpoint: url,
		attributes: &pushAttributes{
			attr: make(map[string]string),
		},
	}
	for k, v := range attributes {
		p.attributes.set(k, v)
	}
	return p, nil
}
