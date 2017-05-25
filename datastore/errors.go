package datastore

import "github.com/pkg/errors"

var (
	// datastore errors
	ErrNotFoundEntry             = errors.New("not found entry")
	ErrInvalidEntry              = errors.New("invalid entry")
	ErrNotMatchTypeMessage       = errors.New("not match type message")
	ErrNotMatchTypeMessageStatus = errors.New("not match type message status")
	ErrNotMatchTypeSubscription  = errors.New("not match type subscription")
	ErrNotMatchTypeTopic         = errors.New("not match type topic")
	ErrNotSupportOperation       = errors.New("not support operation")
	ErrNotSupportDriver          = errors.New("not support driver")
)
