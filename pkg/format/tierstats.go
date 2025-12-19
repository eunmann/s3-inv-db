package format

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/eunmann/s3-inv-db/pkg/triebuild"
)

const tierStatsDir = "tier_stats"

// TierStatsWriter writes per-tier columnar arrays.
type TierStatsWriter struct {
	outDir       string
	tierDir      string
	presentTiers []tiers.ID
}

// NewTierStatsWriter creates a writer for tier statistics.
func NewTierStatsWriter(outDir string) (*TierStatsWriter, error) {
	tierDir := filepath.Join(outDir, tierStatsDir)
	if err := os.MkdirAll(tierDir, 0755); err != nil {
		return nil, fmt.Errorf("create tier_stats dir: %w", err)
	}

	return &TierStatsWriter{
		outDir:  outDir,
		tierDir: tierDir,
	}, nil
}

// Write writes tier statistics from the trie result.
func (w *TierStatsWriter) Write(result *triebuild.Result) error {
	if !result.TrackTiers || len(result.PresentTiers) == 0 {
		return nil // No tier data to write
	}

	mapping := tiers.NewMapping()

	for _, tierID := range result.PresentTiers {
		info := mapping.ByID(tierID)

		// Write bytes array
		bytesPath := filepath.Join(w.tierDir, info.FilePrefix+"_bytes.u64")
		if err := w.writeTierArray(bytesPath, result.Nodes, tierID, true); err != nil {
			return fmt.Errorf("write %s bytes: %w", info.Name, err)
		}

		// Write counts array
		countsPath := filepath.Join(w.tierDir, info.FilePrefix+"_count.u64")
		if err := w.writeTierArray(countsPath, result.Nodes, tierID, false); err != nil {
			return fmt.Errorf("write %s counts: %w", info.Name, err)
		}
	}

	w.presentTiers = result.PresentTiers

	// Write tiers manifest
	if err := tiers.WriteManifest(w.outDir, result.PresentTiers); err != nil {
		return fmt.Errorf("write tier manifest: %w", err)
	}

	return nil
}

func (w *TierStatsWriter) writeTierArray(path string, nodes []triebuild.Node, tierID tiers.ID, isBytes bool) error {
	writer, err := NewArrayWriter(path, 8)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		var val uint64
		if isBytes {
			val = node.TierBytes[tierID]
		} else {
			val = node.TierCounts[tierID]
		}
		if err := writer.WriteU64(val); err != nil {
			writer.Close()
			return err
		}
	}

	return writer.Close()
}

// PresentTiers returns the tiers that were written.
func (w *TierStatsWriter) PresentTiers() []tiers.ID {
	return w.presentTiers
}

// TierStatsReader reads per-tier columnar arrays.
type TierStatsReader struct {
	tierDir      string
	manifest     *tiers.TierManifest
	bytesArrays  map[tiers.ID]*ArrayReader
	countsArrays map[tiers.ID]*ArrayReader
}

// OpenTierStats opens tier statistics from an index directory.
// Returns nil if no tier data exists.
func OpenTierStats(indexDir string) (*TierStatsReader, error) {
	manifest, err := tiers.ReadManifest(indexDir)
	if err != nil {
		return nil, err
	}
	if manifest == nil || len(manifest.Tiers) == 0 {
		return nil, nil // No tier data
	}

	tierDir := filepath.Join(indexDir, tierStatsDir)

	r := &TierStatsReader{
		tierDir:      tierDir,
		manifest:     manifest,
		bytesArrays:  make(map[tiers.ID]*ArrayReader),
		countsArrays: make(map[tiers.ID]*ArrayReader),
	}

	// Open all tier arrays using ArrayReader (handles headers properly)
	for _, tier := range manifest.Tiers {
		bytesPath := filepath.Join(tierDir, tier.FilePrefix+"_bytes.u64")
		bytesReader, err := OpenArray(bytesPath)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("open %s bytes: %w", tier.Name, err)
		}
		r.bytesArrays[tier.ID] = bytesReader

		countsPath := filepath.Join(tierDir, tier.FilePrefix+"_count.u64")
		countsReader, err := OpenArray(countsPath)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("open %s counts: %w", tier.Name, err)
		}
		r.countsArrays[tier.ID] = countsReader
	}

	return r, nil
}

// TierBreakdown represents statistics for a single tier.
type TierBreakdown struct {
	TierID      tiers.ID
	TierName    string
	Bytes       uint64
	ObjectCount uint64
}

// GetBreakdown returns the tier breakdown for a given position.
func (r *TierStatsReader) GetBreakdown(pos uint64) []TierBreakdown {
	if r == nil || r.manifest == nil {
		return nil
	}

	breakdown := make([]TierBreakdown, 0, len(r.manifest.Tiers))

	for _, tier := range r.manifest.Tiers {
		var bytes, count uint64

		if reader, ok := r.bytesArrays[tier.ID]; ok && pos < reader.Count() {
			bytes = reader.UnsafeGetU64(pos)
		}
		if reader, ok := r.countsArrays[tier.ID]; ok && pos < reader.Count() {
			count = reader.UnsafeGetU64(pos)
		}

		// Only include tiers with data at this position
		if bytes > 0 || count > 0 {
			breakdown = append(breakdown, TierBreakdown{
				TierID:      tier.ID,
				TierName:    tier.Name,
				Bytes:       bytes,
				ObjectCount: count,
			})
		}
	}

	return breakdown
}

// GetBreakdownAll returns the tier breakdown for all present tiers (including zeros).
func (r *TierStatsReader) GetBreakdownAll(pos uint64) []TierBreakdown {
	if r == nil || r.manifest == nil {
		return nil
	}

	breakdown := make([]TierBreakdown, 0, len(r.manifest.Tiers))

	for _, tier := range r.manifest.Tiers {
		var bytes, count uint64

		if reader, ok := r.bytesArrays[tier.ID]; ok && pos < reader.Count() {
			bytes = reader.UnsafeGetU64(pos)
		}
		if reader, ok := r.countsArrays[tier.ID]; ok && pos < reader.Count() {
			count = reader.UnsafeGetU64(pos)
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

// HasTierData returns whether tier data is available.
func (r *TierStatsReader) HasTierData() bool {
	return r != nil && r.manifest != nil && len(r.manifest.Tiers) > 0
}

// PresentTiers returns the list of tiers with data.
func (r *TierStatsReader) PresentTiers() []tiers.Info {
	if r == nil || r.manifest == nil {
		return nil
	}
	return r.manifest.Tiers
}

// Close releases all mmap'd resources.
func (r *TierStatsReader) Close() error {
	if r == nil {
		return nil
	}

	for _, reader := range r.bytesArrays {
		if reader != nil {
			reader.Close()
		}
	}
	for _, reader := range r.countsArrays {
		if reader != nil {
			reader.Close()
		}
	}

	r.bytesArrays = nil
	r.countsArrays = nil

	return nil
}
