// Package errors provides common error types used throughout YottaDisk.
package errors

import (
	"errors"
)

// Common errors.
var (
	ErrHeadNotFound     = errors.New("YTFS: head not found")
	ErrDataNotFound     = errors.New("YTFS: data not found")
	ErrConfigCache      = errors.New("YTFS: Cache size config error")
	ErrStorageSize		= New("YTFS: storage size does not meet settings")
)

// New returns an error that formats as the given text.
func New(text string) error {
	return errors.New(text)
}
