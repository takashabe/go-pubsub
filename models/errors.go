package models

import "github.com/pkg/errors"

var (
	// topic errors
	ErrAlreadyExistTopic = errors.New("already exist topic")

	// subscription errors
	ErrAlreadyExistSubscription = errors.New("already exist subscription")
	ErrNotFoundAckID            = errors.New("not found message dependent to ack id")
	ErrInvalidEndpoint          = errors.New("invalid endpoint URL format")

	// message errors
	ErrEmptyMessage       = errors.New("empty message")
	ErrNotYetReceivedAck  = errors.New("not yet received ack")
	ErrAlreadyReadMessage = errors.New("already read message")

	// datastore errors
	ErrNotFoundMessage           = errors.New("not found message")
	ErrNotFoundMessageStatus     = errors.New("not found message status")
	ErrNotMatchTypeMessage       = errors.New("not match type message")
	ErrNotMatchTypeMessageStatus = errors.New("not match type message status")
	ErrNotFoundSubscription      = errors.New("not found subscription")
	ErrNotMatchTypeSubscription  = errors.New("not match type subscription")
	ErrNotFoundTopic             = errors.New("not found topic")
	ErrNotMatchTypeTopic         = errors.New("not match type topic")
	ErrNotSupportOperation       = errors.New("not support operation")
)
