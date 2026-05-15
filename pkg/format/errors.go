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
	// ErrSizeMismatch indicates a file size mismatch between a manifest entry and the on-disk file.
	ErrSizeMismatch = errors.New("size mismatch")
	// ErrChecksumMismatch indicates a file checksum did not match the manifest entry.
	ErrChecksumMismatch = errors.New("checksum mismatch")
	// ErrFileTooSmall indicates a file is smaller than its declared header reports.
	ErrFileTooSmall = errors.New("file too small")
	// ErrWidthMismatch indicates an array width does not match the requested operation width.
	ErrWidthMismatch = errors.New("width mismatch")
	// ErrHashCollision indicates two distinct inputs produced the same hash key.
	ErrHashCollision = errors.New("hash collision")
	// ErrMPHFLookupFailed indicates a Find call into the MPHF returned no result for a known prefix.
	ErrMPHFLookupFailed = errors.New("MPHF lookup failed")
	// ErrMPHFAmbiguousKey indicates the MPHF returned 0 (ambiguous sentinel) for an existing key.
	ErrMPHFAmbiguousKey = errors.New("MPHF key returned sentinel zero")
	// ErrMPHFUnknownHash indicates the MPHF returned a hash not present in the input set.
	ErrMPHFUnknownHash = errors.New("MPHF returned unknown hash")
	// ErrPrefixBlobNotLoaded indicates no prefix blob (raw or segmented) is loaded.
	ErrPrefixBlobNotLoaded = errors.New("prefix blob not loaded")
	// ErrNoPrefixStorage indicates the MPHF has no prefix storage attached.
	ErrNoPrefixStorage = errors.New("no prefix storage loaded")
	// ErrLookupWrongPos indicates a verification lookup returned an unexpected position.
	ErrLookupWrongPos = errors.New("lookup returned wrong pos")
)
