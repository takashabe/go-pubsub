package models

import "github.com/pkg/errors"

var (
	ErrNotSupportOperation      = errors.New("not support operation")
	ErrInvalidEndpoint          = errors.New("invalid endpoint URL format")
	ErrEmptyMessage             = errors.New("empty message")
	ErrAlreadyExistTopic        = errors.New("already exist topic")
	ErrAlreadyExistSubscription = errors.New("already exist subscription")

	// datastore errors
	ErrNotFoundMessage          = errors.New("not found message")
	ErrNotMatchTypeMessage      = errors.New("not match type message")
	ErrNotFoundSubscription     = errors.New("not found subscription")
	ErrNotMatchTypeSubscription = errors.New("not match type subscription")
	ErrNotFoundTopic            = errors.New("not found topic")
	ErrNotMatchTypeTopic        = errors.New("not match type topic")
)
