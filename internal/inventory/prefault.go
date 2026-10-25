package inventory

import (
	"fmt"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
)

// prefaultMemoryShare is how much of system memory one index may take
// to be prefaulted when the server opens it: at most 1/8, so reading it
// in doesn't push the other loaded indexes out of the page cache.
const prefaultMemoryShare = 8

// openServed opens an index the server will serve. An index that fits
// in prefaultMemoryShare of system memory is read into the page cache
// first, so the first queries after a load or a restart don't wait on
// storage (open plus the first 1000 lookups -53% on the benchmark
// fixture, #55); a larger one opens as usual.
func openServed(dir string) (*indexread.Index, error) {
	idx, err := indexread.OpenWith(dir, indexread.Options{Prefault: fitsPrefault(dir, sysmem.Total().TotalBytes)})
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}

	return idx, nil
}

// fitsPrefault reports whether the index in dir is small enough to
// prefault on a host with memBytes of memory. Without a readable
// manifest it doesn't prefault.
func fitsPrefault(dir string, memBytes uint64) bool {
	m, err := format.ReadManifest(dir)
	if err != nil {
		return false
	}

	return m.TotalBytes() <= memBytes/prefaultMemoryShare
}
