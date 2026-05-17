package format

import (
	"errors"
	"fmt"
	"path/filepath"
)

// DepthIndexBuilder accumulates positions per depth and writes the
// depth posting lists at Finalize. Storage is **disk-backed per
// depth** via u64DiskArray: at billion-prefix scale the positions
// would otherwise be ~8 GiB of heap (1B × 8 B). Each per-depth
// array is append-only and grows monotonically because positions
// are added in preorder (pos = 0, 1, 2, ...), so within any one
// depth the values are already sorted — no post-add sort needed.
type DepthIndexBuilder struct {
	tempDir  string
	buckets  []*u64DiskArray // index = depth
	maxDepth uint32
}

// NewDepthIndexBuilder creates a depth index builder backed by
// per-depth disk arrays. The tempDir is where per-depth scratch
// files live; cleaned up by Build (after the final arrays are
// written) or by Close on the error path.
func NewDepthIndexBuilder(tempDir string) *DepthIndexBuilder {
	return &DepthIndexBuilder{tempDir: tempDir}
}

// Add appends pos to the per-depth posting list. Caller must add
// in pos order — positions within a depth are written in arrival
// order and Build relies on that order being already sorted.
func (b *DepthIndexBuilder) Add(pos uint64, depth uint32) error {
	for uint32(len(b.buckets)) <= depth {
		a, err := newU64DiskArray(b.tempDir, fmt.Sprintf("depth_%02d", len(b.buckets)))
		if err != nil {
			return fmt.Errorf("create depth bucket %d: %w", len(b.buckets), err)
		}
		b.buckets = append(b.buckets, a)
	}
	if err := b.buckets[depth].Append(pos); err != nil {
		return fmt.Errorf("append depth %d pos: %w", depth, err)
	}
	if depth > b.maxDepth {
		b.maxDepth = depth
	}

	return nil
}

// Build writes the depth index files (depth_offsets.u64 +
// depth_positions.u64), concatenating per-depth disk buckets in
// depth order. Each bucket is read once and copied to the final
// file. Per-bucket scratch is closed and removed as we go.
func (b *DepthIndexBuilder) Build(outDir string) error {
	offsetsPath := filepath.Join(outDir, "depth_offsets.u64")
	positionsPath := filepath.Join(outDir, "depth_positions.u64")

	offsetsWriter, err := NewArrayWriter(offsetsPath, 8)
	if err != nil {
		return fmt.Errorf("create offsets writer: %w", err)
	}
	positionsWriter, err := NewArrayWriter(positionsPath, 8)
	if err != nil {
		offsetsWriter.Close()

		return fmt.Errorf("create positions writer: %w", err)
	}

	offset := uint64(0)
	var errs []error
	for d := uint32(0); d <= b.maxDepth; d++ {
		if err := offsetsWriter.WriteU64(offset); err != nil {
			errs = append(errs, fmt.Errorf("write offset depth %d: %w", d, err))

			break
		}
		var bucket *u64DiskArray
		if int(d) < len(b.buckets) {
			bucket = b.buckets[d]
		}
		if bucket == nil {
			continue
		}
		if err := bucket.Freeze(); err != nil {
			errs = append(errs, fmt.Errorf("freeze depth %d: %w", d, err))

			break
		}
		positions := bucket.Slice()
		for _, p := range positions {
			if err := positionsWriter.WriteU64(p); err != nil {
				errs = append(errs, fmt.Errorf("write position depth %d: %w", d, err))

				break
			}
		}
		offset += uint64(len(positions))
		bucket.Close()
		b.buckets[d] = nil
	}

	if err := offsetsWriter.WriteU64(offset); err != nil {
		errs = append(errs, fmt.Errorf("write sentinel offset: %w", err))
	}
	if err := positionsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close positions: %w", err))
	}
	if err := offsetsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close offsets: %w", err))
	}

	return errors.Join(errs...)
}

// Close releases any open per-depth buckets without writing the
// output. Used on the error path; idempotent.
func (b *DepthIndexBuilder) Close() error {
	var errs []error
	for i, bucket := range b.buckets {
		if bucket == nil {
			continue
		}
		if err := bucket.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close depth %d bucket: %w", i, err))
		}
		b.buckets[i] = nil
	}

	return errors.Join(errs...)
}

// MaxDepth returns the maximum depth seen.
func (b *DepthIndexBuilder) MaxDepth() uint32 {
	return b.maxDepth
}

// DepthIndex provides read access to depth posting lists.
//
// Thread Safety: DepthIndex is safe for concurrent read access from multiple
// goroutines. All read methods can be called concurrently. Close should only
// be called once, after all read operations have completed.
type DepthIndex struct {
	offsets   *ArrayReader
	positions *ArrayReader
	maxDepth  uint32
}

// OpenDepthIndex opens a depth index from files.
func OpenDepthIndex(outDir string) (*DepthIndex, error) {
	offsetsPath := filepath.Join(outDir, "depth_offsets.u64")
	positionsPath := filepath.Join(outDir, "depth_positions.u64")

	// Depth-index offsets are read at well-known indices (one per
	// depth) then once per query; small enough that hint doesn't
	// matter, but keep sequential for symmetry with positions.
	offsets, err := OpenArrayWithHint(offsetsPath, AccessHintSequential)
	if err != nil {
		return nil, fmt.Errorf("open offsets: %w", err)
	}

	// Depth positions are scanned in contiguous ranges during browse
	// iteration — every prefix at a given depth in subtree order.
	// Sequential hint enables kernel readahead.
	positions, err := OpenArrayWithHint(positionsPath, AccessHintSequential)
	if err != nil {
		offsets.Close()

		return nil, fmt.Errorf("open positions: %w", err)
	}

	// maxDepth = number of offsets - 2 (because we have maxDepth+2 entries)
	maxDepth := uint32(0)
	if offsets.Count() > 1 {
		maxDepth = uint32(offsets.Count() - 2)
	}

	return &DepthIndex{
		offsets:   offsets,
		positions: positions,
		maxDepth:  maxDepth,
	}, nil
}

// Close releases resources.
func (d *DepthIndex) Close() error {
	return errors.Join(d.offsets.Close(), d.positions.Close())
}

// MaxDepth returns the maximum depth in the index.
func (d *DepthIndex) MaxDepth() uint32 {
	return d.maxDepth
}

// PositionsInSubtree returns positions at the given depth that fall within
// the subtree [subtreeStart, subtreeEnd]. Uses binary search.
func (d *DepthIndex) PositionsInSubtree(depth uint32, subtreeStart, subtreeEnd uint64) ([]uint64, error) {
	if depth > d.maxDepth {
		return nil, nil
	}

	start, err := d.offsets.GetU64(uint64(depth))
	if err != nil {
		return nil, fmt.Errorf("get start offset for depth %d: %w", depth, err)
	}

	end, err := d.offsets.GetU64(uint64(depth + 1))
	if err != nil {
		return nil, fmt.Errorf("get end offset for depth %d: %w", depth, err)
	}

	if start >= end {
		return nil, nil
	}

	// Binary search for lower bound (first pos >= subtreeStart)
	lo := d.binarySearchLower(start, end, subtreeStart)

	// Binary search for upper bound (first pos > subtreeEnd)
	hi := d.binarySearchUpper(lo, end, subtreeEnd)

	count := hi - lo
	if count == 0 {
		return nil, nil
	}

	positions := make([]uint64, count)
	for i := range count {
		positions[i] = d.positions.UnsafeGetU64(lo + i)
	}

	return positions, nil
}

// binarySearchLower finds the first index where positions[idx] >= target.
func (d *DepthIndex) binarySearchLower(start, end, target uint64) uint64 {
	lo, hi := start, end
	for lo < hi {
		mid := lo + (hi-lo)/2
		val := d.positions.UnsafeGetU64(mid)
		if val < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// binarySearchUpper finds the first index where positions[idx] > target.
func (d *DepthIndex) binarySearchUpper(start, end, target uint64) uint64 {
	lo, hi := start, end
	for lo < hi {
		mid := lo + (hi-lo)/2
		val := d.positions.UnsafeGetU64(mid)
		if val <= target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}
