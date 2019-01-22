package errors

import (
	"errors"
)

// Common errors.
var (
	ErrHeadNotFound     = errors.New("yotta-disk: head not found")
	ErrDataNotFound     = errors.New("yotta-disk: data not found")
	ErrConfigCache      = errors.New("yotta-disk: Cache size config error")
	ErrStorageSize		= New("yotta-disk: storage size does not meet settings")
)

// New returns an error that formats as the given text.
func New(text string) error {
	return errors.New(text)
}
