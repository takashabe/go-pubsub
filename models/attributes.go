package models

import "sync"

// Attributes is string key-value map. optional for the message, push...
type Attributes struct {
	attr map[string]string
	mu   sync.Mutex
}

func (a *Attributes) Set(key, value string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.attr[key] = value
}

func (a *Attributes) Get(key string) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.attr[key]
	return v, ok
}

func (a *Attributes) Dump() map[string]string {
	return a.attr
}

func newAttributes(attr map[string]string) *Attributes {
	return &Attributes{
		attr: attr,
	}
}
