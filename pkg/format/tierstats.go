package format

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TierStatsReader reads per-prefix tier breakdowns from either the
// dense tier_stats_row.bin or the sparse pair (tier_stats_sparse.bin
// + tier_stats_sparse.off.u64). Exactly one is on disk per index;
// OpenTierStats picks based on file presence.
type TierStatsReader struct {
	manifest     *tiers.Manifest
	rowReader    *TierStatsRowReader
	sparseReader *TierStatsSparseReader
}

// OpenTierStats opens the tier-stats file from an index directory.
// Returns an empty reader (manifest with zero tiers) when no tier
// data exists, so callers can dispatch on
// (*TierStatsReader).HasTierData() rather than nil-checking.
func OpenTierStats(indexDir string) (*TierStatsReader, error) {
	manifest, err := tiers.ReadManifest(indexDir)
	if err != nil {
		return nil, fmt.Errorf("read tier manifest: %w", err)
	}
	if manifest == nil || len(manifest.Tiers) == 0 {
		return &TierStatsReader{manifest: &tiers.Manifest{}}, nil
	}

	if tierStatsSparsePresent(indexDir) {
		sparse, err := OpenTierStatsSparse(indexDir, len(manifest.Tiers))
		if err != nil {
			return nil, fmt.Errorf("open tier stats sparse: %w", err)
		}

		return &TierStatsReader{
			manifest:     manifest,
			sparseReader: sparse,
		}, nil
	}

	rowReader, err := OpenTierStatsRow(indexDir)
	if err != nil {
		return nil, fmt.Errorf("open tier stats row: %w", err)
	}
	if rowReader.SlotCount() != len(manifest.Tiers) {
		rowReader.Close()

		return nil, fmt.Errorf("%w: row file has %d slots, manifest has %d tiers",
			errTierStatsRowWidth, rowReader.SlotCount(), len(manifest.Tiers))
	}

	return &TierStatsReader{
		manifest:  manifest,
		rowReader: rowReader,
	}, nil
}

// TierBreakdown represents statistics for a single tier.
type TierBreakdown struct {
	TierName    string
	Bytes       uint64
	ObjectCount uint64
	TierID      tiers.ID
}

// Breakdown returns the tier breakdown for the given preorder
// position, including only tiers with non-zero data.
func (r *TierStatsReader) Breakdown(pos uint64) []TierBreakdown {
	return r.breakdownAt(pos, true)
}

// BreakdownAll returns the tier breakdown for all present tiers
// at the given preorder position, including zeros.
func (r *TierStatsReader) BreakdownAll(pos uint64) []TierBreakdown {
	return r.breakdownAt(pos, false)
}

func (r *TierStatsReader) breakdownAt(pos uint64, nonZeroOnly bool) []TierBreakdown {
	if r == nil || r.manifest == nil {
		return nil
	}
	if r.sparseReader != nil {
		return r.sparseBreakdownAt(pos, nonZeroOnly)
	}
	if r.rowReader == nil {
		return nil
	}
	breakdown := make([]TierBreakdown, 0, len(r.manifest.Tiers))
	if pos >= r.rowReader.Count() {
		return breakdown
	}
	row := r.rowReader.UnsafeRow(pos)
	// Slot order in the packed file is manifest order, not tier-ID
	// order: slot i corresponds to manifest.Tiers[i].
	for slotIdx, tier := range r.manifest.Tiers {
		off := slotIdx * TierStatsSlotBytes
		count := binary.LittleEndian.Uint64(row[off : off+8])
		bytes := binary.LittleEndian.Uint64(row[off+8 : off+16])
		if nonZeroOnly && bytes == 0 && count == 0 {
			continue
		}
		breakdown = append(breakdown, TierBreakdown{
			TierID:      tier.ID,
			TierName:    tier.Name,
			Bytes:       bytes,
			ObjectCount: count,
		})
	}

	return breakdown
}

// sparseBreakdownAt produces a TierBreakdown slice from the sparse
// reader. For nonZeroOnly=false (BreakdownAll), absent slots are
// emitted as zero-valued entries to match the dense path's contract.
func (r *TierStatsReader) sparseBreakdownAt(pos uint64, nonZeroOnly bool) []TierBreakdown {
	if pos >= r.sparseReader.Count() {
		return make([]TierBreakdown, 0, len(r.manifest.Tiers))
	}
	// Materialize the row into per-slot (count, bytes); the visit
	// callback fills only the populated ones.
	type cb struct{ count, bytes uint64 }
	scratch := make([]cb, len(r.manifest.Tiers))
	r.sparseReader.VisitRow(pos, func(slotIdx int, count, bytesV uint64) {
		if slotIdx < len(scratch) {
			scratch[slotIdx] = cb{count: count, bytes: bytesV}
		}
	})

	breakdown := make([]TierBreakdown, 0, len(r.manifest.Tiers))
	for slotIdx, tier := range r.manifest.Tiers {
		s := scratch[slotIdx]
		if nonZeroOnly && s.count == 0 && s.bytes == 0 {
			continue
		}
		breakdown = append(breakdown, TierBreakdown{
			TierID:      tier.ID,
			TierName:    tier.Name,
			Bytes:       s.bytes,
			ObjectCount: s.count,
		})
	}

	return breakdown
}

// HasTierData reports whether any tier data is available.
func (r *TierStatsReader) HasTierData() bool {
	return r != nil && r.manifest != nil && len(r.manifest.Tiers) > 0
}

// Close releases the mmap'd row / sparse files.
func (r *TierStatsReader) Close() error {
	if r == nil {
		return nil
	}
	var rowErr, sparseErr error
	if r.rowReader != nil {
		rowErr = r.rowReader.Close()
		r.rowReader = nil
	}
	if r.sparseReader != nil {
		sparseErr = r.sparseReader.Close()
		r.sparseReader = nil
	}

	return errors.Join(rowErr, sparseErr)
}
