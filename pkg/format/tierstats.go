package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

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
// reader. For nonZeroOnly=true (Breakdown), only populated slots are
// returned. For nonZeroOnly=false (BreakdownAll), absent slots are
// emitted as zero-valued entries in their dense position.
//
// Hot path makes a single allocation (the result slice). The bitmap
// + populated cells are decoded into a fixed-size on-stack array,
// matching the dense path's allocation profile.
func (r *TierStatsReader) sparseBreakdownAt(pos uint64, nonZeroOnly bool) []TierBreakdown {
	if pos >= r.sparseReader.Count() {
		return make([]TierBreakdown, 0, len(r.manifest.Tiers))
	}

	// tierStatsSparseBitmapBytes*8 == 16, well above the project's
	// 13-tier maximum, so this stack array always covers every slot.
	const maxSlots = tierStatsSparseBitmapBytes * 8
	var counts, bytesArr [maxSlots]uint64
	bitmap := r.sparseReader.fillRow(pos, &counts, &bytesArr)

	estCap := bits.OnesCount16(bitmap)
	if !nonZeroOnly {
		estCap = len(r.manifest.Tiers)
	}
	breakdown := make([]TierBreakdown, 0, estCap)
	for slotIdx, tier := range r.manifest.Tiers {
		populated := bitmap&(1<<slotIdx) != 0
		if nonZeroOnly && !populated {
			continue
		}
		breakdown = append(breakdown, TierBreakdown{
			TierID:      tier.ID,
			TierName:    tier.Name,
			Bytes:       bytesArr[slotIdx],
			ObjectCount: counts[slotIdx],
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
