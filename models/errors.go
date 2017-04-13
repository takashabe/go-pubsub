package models

import "github.com/pkg/errors"

var (
	ErrNotSupportOperation      = errors.New("not support operation")
	ErrInvalidEndpoint          = errors.New("invalid endpoint URL format")
	ErrAlreadyExistTopic        = errors.New("already exist topic")
	ErrAlreadyExistSubscription = errors.New("already exist subscription")

	// message errors
	ErrEmptyMessage      = errors.New("empty message")
	ErrNotYetReceivedAck = errors.New("not yet received ack")

	// datastore errors
	ErrNotFoundMessage          = errors.New("not found message")
	ErrNotMatchTypeMessage      = errors.New("not match type message")
	ErrNotFoundSubscription     = errors.New("not found subscription")
	ErrNotMatchTypeSubscription = errors.New("not match type subscription")
	ErrNotFoundTopic            = errors.New("not found topic")
	ErrNotMatchTypeTopic        = errors.New("not match type topic")
)
