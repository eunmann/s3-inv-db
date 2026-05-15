// Package membudget provides parsing of human-readable byte-size
// strings (e.g., "4GiB", "200GB"). The package historically held a
// per-stage memory budget for the build pipeline; that was retired in
// favour of GOMEMLIMIT-driven flow control (see pkg/sysmem and
// pkg/extsort.ShouldFlush).
package membudget

import (
	"errors"
	"fmt"
)

// Sentinel errors surfaced by ParseHumanSize. Wrapped with %w by the
// parser so callers can errors.Is them out of a wrapped chain.
var (
	// ErrEmptySize is returned when ParseHumanSize is called on "".
	ErrEmptySize = errors.New("empty size string")
	// ErrInvalidSizeNumber is returned when the numeric prefix of a
	// size string fails to parse as a float.
	ErrInvalidSizeNumber = errors.New("invalid size number")
	// ErrUnknownSizeSuffix is returned when the suffix following the
	// number isn't one of the recognised SI/IEC units.
	ErrUnknownSizeSuffix = errors.New("unknown size suffix")
)

// Byte-multiplier constants for the ParseHumanSize suffix table.
// Split out as named constants so the magic-number linter has names to
// bind to instead of bare literals in the map initialiser.
const (
	bytesKB  = 1e3
	bytesKiB = 1 << 10
	bytesMB  = bytesKB * bytesKB
	bytesMiB = bytesKiB * bytesKiB
	bytesGB  = bytesMB * bytesKB
	bytesGiB = bytesMiB * bytesKiB
	bytesTB  = bytesGB * bytesKB
	bytesTiB = bytesGiB * bytesKiB
)

// sizeSuffixMultipliers maps every accepted ParseHumanSize unit suffix
// to its byte multiplier. Map-based lookup keeps the parser's
// cyclomatic complexity low.
//
//nolint:gochecknoglobals // multiplier table; immutable after init.
var sizeSuffixMultipliers = map[string]float64{
	"":    1,
	"B":   1,
	"KB":  bytesKB,
	"KiB": bytesKiB,
	"K":   bytesKiB,
	"MB":  bytesMB,
	"MiB": bytesMiB,
	"M":   bytesMiB,
	"GB":  bytesGB,
	"GiB": bytesGiB,
	"G":   bytesGiB,
	"TB":  bytesTB,
	"TiB": bytesTiB,
	"T":   bytesTiB,
}

// ParseHumanSize parses a human-readable size string (e.g., "4GiB", "512MB").
// Supported suffixes: B, KB, KiB, MB, MiB, GB, GiB, TB, TiB.
func ParseHumanSize(s string) (uint64, error) {
	if s == "" {
		return 0, ErrEmptySize
	}
	numEnd := numericPrefixEnd(s)
	numStr := s[:numEnd]
	suffix := s[numEnd:]

	var num float64
	if _, err := fmt.Sscanf(numStr, "%f", &num); err != nil {
		return 0, fmt.Errorf("%w %q: %w", ErrInvalidSizeNumber, numStr, err)
	}
	mult, ok := sizeSuffixMultipliers[suffix]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrUnknownSizeSuffix, suffix)
	}

	return uint64(num * mult), nil
}

// numericPrefixEnd returns the byte offset of the first non-numeric
// character in s. The number portion of a size string ends here and
// the unit suffix begins.
func numericPrefixEnd(s string) int {
	for i, c := range s {
		if (c < '0' || c > '9') && c != '.' {
			return i
		}
	}

	return len(s)
}
