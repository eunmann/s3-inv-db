package extsort

import "errors"

// Sentinel errors for the extsort package. Wrap with %w when adding context
// via fmt.Errorf so callers can match with errors.Is.
var (
	// ErrInvalidMagic indicates a run file has the wrong magic number.
	ErrInvalidMagic = errors.New("invalid magic")
	// ErrUnsupportedVersion indicates an unsupported run file format version.
	ErrUnsupportedVersion = errors.New("unsupported run file version")
	// ErrNotCompressed indicates the file is not compressed when a
	// compressed reader is required.
	ErrNotCompressed = errors.New("file is not compressed")
	// ErrUnsupportedCompression indicates an unsupported compression type.
	ErrUnsupportedCompression = errors.New("unsupported compression type")
	// ErrNoInputPaths indicates a merge call received no input paths.
	ErrNoInputPaths = errors.New("no input paths provided")
)
