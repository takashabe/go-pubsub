package queue

import "time"

type Value []byte

type Message struct {
	Topic   string
	Value   Value
	timeout time.Duration
}
