package sqliteagg

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// newInventoryReader creates a csv.Reader configured for S3 inventory files.
// The reader is configured to:
//   - Reuse record slices for performance
//   - Accept variable field counts (FieldsPerRecord = -1)
//   - Handle lazy quotes common in inventory files
func newInventoryReader(r io.Reader) *csv.Reader {
	csvr := csv.NewReader(r)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true
	return csvr
}

// decompressReader wraps a reader with gzip decompression if the key ends in .gz.
// Returns the reader (possibly wrapped), a closer function that must be called,
// and any error. The closer may be nil if no decompression wrapper was added.
func decompressReader(r io.Reader, key string) (io.Reader, func() error, error) {
	if !strings.HasSuffix(strings.ToLower(key), ".gz") {
		return r, nil, nil
	}

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, nil, fmt.Errorf("create gzip reader: %w", err)
	}
	return gzr, gzr.Close, nil
}
