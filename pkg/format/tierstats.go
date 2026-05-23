package format

import (
	"encoding/binary"
	"fmt"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

const tierStatsDir = "tier_stats"

// TierStatsReader reads per-prefix tier breakdowns from the row-major
// tier_stats_row.bin file. Production writers (TierStatsRowWriter)
// always produce this layout; the legacy per-tier columnar layout
// is no longer supported.
type TierStatsReader struct {
	manifest  *tiers.TierManifest
	rowReader *TierStatsRowReader
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
		return &TierStatsReader{manifest: &tiers.TierManifest{}}, nil
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
	if r == nil || r.manifest == nil || r.rowReader == nil {
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

// HasTierData reports whether any tier data is available.
func (r *TierStatsReader) HasTierData() bool {
	return r != nil && r.manifest != nil && len(r.manifest.Tiers) > 0
}

// Close releases the mmap'd row file.
func (r *TierStatsReader) Close() error {
	if r == nil || r.rowReader == nil {
		return nil
	}
	err := r.rowReader.Close()
	r.rowReader = nil
	if err != nil {
		return fmt.Errorf("close tier stats row: %w", err)
	}

	return nil
}
