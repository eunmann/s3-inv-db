package format

import "errors"

var (
	// ErrInvalidHeader indicates an invalid or corrupted file header.
	ErrInvalidHeader = errors.New("invalid file header")
	// ErrMagicMismatch indicates the magic number doesn't match.
	ErrMagicMismatch = errors.New("magic number mismatch")
	// ErrVersionMismatch indicates an unsupported format version.
	ErrVersionMismatch = errors.New("unsupported format version")
	// ErrBoundsCheck indicates an out-of-bounds access attempt.
	ErrBoundsCheck = errors.New("index out of bounds")
)
