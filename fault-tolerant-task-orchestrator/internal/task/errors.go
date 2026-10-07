package task

import (
	"errors"
	"fmt"
)

// TemporaryError indicates an error that may be resolved by retrying after a backoff.
// Examples: network timeouts, temporary service unavailability, lock acquisition failure.
type TemporaryError struct {
	Err error
}

func (e *TemporaryError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("temporary error: %v", e.Err)
	}
	return "temporary error"
}

func (e *TemporaryError) Unwrap() error {
	return e.Err
}

// NewTemporaryError wraps an underlying error as a TemporaryError.
func NewTemporaryError(err error) error {
	return &TemporaryError{Err: err}
}

// FatalError indicates an unrecoverable error that should not be retried.
// Examples: malformed payload, schema mismatch, unauthorized request.
type FatalError struct {
	Err error
}

func (e *FatalError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("fatal error: %v", e.Err)
	}
	return "fatal error"
}

func (e *FatalError) Unwrap() error {
	return e.Err
}

// NewFatalError wraps an underlying error as a FatalError.
func NewFatalError(err error) error {
	return &FatalError{Err: err}
}

// IsTemporary checks if the error or any wrapped error is a TemporaryError.
func IsTemporary(err error) bool {
	var tempErr *TemporaryError
	return errors.As(err, &tempErr)
}

// IsFatal checks if the error or any wrapped error is a FatalError.
func IsFatal(err error) bool {
	var fatalErr *FatalError
	return errors.As(err, &fatalErr)
}
