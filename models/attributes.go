package models

import (
	"fmt"
	"sync"
)

// Attributes is string key-value map. optional for the message, push...
type Attributes struct {
	Attr map[string]string
	mu   sync.Mutex
}

func (a *Attributes) Set(key, value string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Attr[key] = value
}

func (a *Attributes) Get(key string) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.Attr[key]
	return v, ok
}

func (a *Attributes) Dump() map[string]string {
	return a.Attr
}

func (a *Attributes) String() string {
	return fmt.Sprint(a.Attr)
}

func newAttributes(attr map[string]string) *Attributes {
	return &Attributes{
		Attr: attr,
	}
}
