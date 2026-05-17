package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// IndexBuilder builds index files directly from a sorted stream of PrefixRows.
// It processes prefixes in a single streaming pass, computing preorder positions
// and subtree ranges on the fly without building an in-memory trie.
//
// The builder uses a depth stack to track ancestors and close subtrees as
// prefixes are processed in sorted (preorder) order.
//
// Memory usage is bounded: prefix strings are written to disk during construction
// (via StreamingMPHFBuilder). Subtree arrays remain in memory (~12 bytes per prefix)
// which is much smaller than storing all prefix strings (~50+ bytes each).
type IndexBuilder struct {
	outDir  string
	tempDir string

	// coreStatsW packs object_count, total_bytes, subtree_end,
	// depth, and max_depth_in_subtree per prefix into one fixed-stride
	// mmap'd file (core_stats.bin). Replaces the previous five
	// separate per-column writers. Random writes (subtree_end +
	// max_depth_in_subtree) happen during subtree close.
	coreStatsW *format.CoreStatsBuilder

	// tierStatsRowW writes all NumTiers × (count, bytes) per prefix
	// into one fixed-stride row file. Replaces the previous
	// per-tier column writers. Reader fall-back still handles the
	// legacy layout.
	tierStatsRowW *format.TierStatsRowWriter

	mphfBuilder       *format.StreamingMPHFBuilder
	depthIndexBuilder *format.DepthIndexBuilder

	stack    []stackEntry
	posCount uint64
	maxDepth uint32

	presentTiers map[tiers.ID]bool

	closed bool
}

// stackEntry tracks an open prefix on the stack.
type stackEntry struct {
	prefix            string
	pos               uint64
	depth             uint32
	maxDepthInSubtree uint32
}

// NewIndexBuilder creates a streaming index builder.
// The tempDir is used for temporary storage during construction (for MPHF builder).
// If tempDir is empty, os.TempDir() is used.
func NewIndexBuilder(outDir, tempDir string) (*IndexBuilder, error) {
	return NewIndexBuilderWithCapacity(outDir, tempDir, 0)
}

// NewIndexBuilderWithCapacity creates a streaming index builder with a capacity hint.
// The capacityHint is used to pre-size internal arrays, reducing allocations when
// the approximate number of prefixes is known (e.g., from a run file header).
// If capacityHint is 0, a small default capacity is used.
func NewIndexBuilderWithCapacity(outDir, tempDir string, capacityHint uint64) (*IndexBuilder, error) {
	if err := os.MkdirAll(outDir, format.DirPerm); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	if tempDir == "" {
		tempDir = os.TempDir()
	}

	mphfBuilder, err := format.NewStreamingMPHFBuilder(tempDir)
	if err != nil {
		return nil, fmt.Errorf("create MPHF builder: %w", err)
	}

	// Use capacity hint for arrays, with a minimum of 1024
	arrayCap := max(capacityHint, uint64(1024))

	coreStatsW, err := format.NewCoreStatsBuilder(outDir, arrayCap)
	if err != nil {
		mphfBuilder.Close()

		return nil, fmt.Errorf("create core stats builder: %w", err)
	}

	b := &IndexBuilder{
		outDir:            outDir,
		tempDir:           tempDir,
		coreStatsW:        coreStatsW,
		mphfBuilder:       mphfBuilder,
		depthIndexBuilder: format.NewDepthIndexBuilder(tempDir),
		stack:             make([]stackEntry, 0, 32),
		presentTiers:      make(map[tiers.ID]bool),
	}

	b.tierStatsRowW, err = format.NewTierStatsRowWriter(outDir)
	if err != nil {
		coreStatsW.Close()
		mphfBuilder.Close()

		return nil, fmt.Errorf("create tier stats row writer: %w", err)
	}

	return b, nil
}

// cleanup closes and removes all partially created files on error.
func (b *IndexBuilder) cleanup() {
	if b.coreStatsW != nil {
		b.coreStatsW.Close()
	}
	if b.mphfBuilder != nil {
		b.mphfBuilder.Close()
	}
	if b.tierStatsRowW != nil {
		b.tierStatsRowW.Close()
	}
	os.RemoveAll(b.outDir)
}

// Add processes a single PrefixRow from the sorted stream.
// Prefixes must be added in lexicographic (sorted) order.
func (b *IndexBuilder) Add(row *PrefixRow) error {
	commonDepth := b.findCommonAncestorDepth(row.Prefix)
	if err := b.closeNodesAbove(commonDepth); err != nil {
		return err
	}

	pos := b.posCount
	b.posCount++

	b.stack = append(b.stack, stackEntry{
		prefix:            row.Prefix,
		pos:               pos,
		depth:             uint32(row.Depth),
		maxDepthInSubtree: uint32(row.Depth),
	})

	if uint32(row.Depth) > b.maxDepth {
		b.maxDepth = uint32(row.Depth)
	}

	if err := b.depthIndexBuilder.Add(pos, uint32(row.Depth)); err != nil {
		return fmt.Errorf("add to depth index: %w", err)
	}
	if err := b.mphfBuilder.Add(row.Prefix, pos); err != nil {
		return fmt.Errorf("add to MPHF builder: %w", err)
	}

	if err := b.coreStatsW.Add(row.Count, row.TotalBytes, row.Depth); err != nil {
		return fmt.Errorf("write core stats row: %w", err)
	}

	for tierID := range tiers.NumTiers {
		if row.TierCounts[tierID] > 0 || row.TierBytes[tierID] > 0 {
			b.presentTiers[tierID] = true
		}
	}

	if err := b.tierStatsRowW.Add(&row.TierCounts, &row.TierBytes); err != nil {
		return fmt.Errorf("write tier stats row: %w", err)
	}

	return nil
}

// findCommonAncestorDepth finds the depth of the deepest common ancestor.
func (b *IndexBuilder) findCommonAncestorDepth(prefix string) int {
	commonDepth := 0

	for i := range len(b.stack) {
		entry := &b.stack[i]
		// Check if stack entry is a prefix of the new prefix
		if len(entry.prefix) <= len(prefix) && prefix[:len(entry.prefix)] == entry.prefix {
			commonDepth = i + 1
		} else {
			break
		}
	}

	return commonDepth
}

// closeNodesAbove closes all stack nodes with depth > targetDepth.
func (b *IndexBuilder) closeNodesAbove(targetDepth int) error {
	for len(b.stack) > targetDepth {
		if err := b.closeTopNode(); err != nil {
			return err
		}
	}

	return nil
}

// closeTopNode closes the node at the top of the stack.
func (b *IndexBuilder) closeTopNode() error {
	if len(b.stack) == 0 {
		return nil
	}

	top := b.stack[len(b.stack)-1]
	b.stack = b.stack[:len(b.stack)-1]

	subtreeEnd := b.posCount - 1
	if err := b.coreStatsW.SetSubtree(top.pos, subtreeEnd, uint16(top.maxDepthInSubtree)); err != nil {
		return fmt.Errorf("set subtree_end: %w", err)
	}

	if len(b.stack) > 0 {
		if top.maxDepthInSubtree > b.stack[len(b.stack)-1].maxDepthInSubtree {
			b.stack[len(b.stack)-1].maxDepthInSubtree = top.maxDepthInSubtree
		}
	}

	return nil
}

// AddAll processes all PrefixRows from an iterator.
func (b *IndexBuilder) AddAll(iter RowIterator) error {
	return b.AddAllWithContext(context.Background(), iter)
}

// AddAllWithContext processes all PrefixRows from an iterator with context support.
// It periodically checks for context cancellation to allow graceful shutdown.
func (b *IndexBuilder) AddAllWithContext(ctx context.Context, iter RowIterator) error {
	const checkInterval = 1000 // Check context every N rows
	const logInterval = 100000 // Log progress every N rows
	log := zerolog.Ctx(ctx)
	count := 0
	startTime := time.Now()
	lastLogTime := startTime

	log.Debug().Msg("index builder: starting AddAllWithContext")

	for {
		// Periodic context check to avoid blocking forever
		if count%checkInterval == 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("index build cancelled: %w", ctx.Err())
			default:
			}
		}

		// Periodic progress logging
		if count > 0 && count%logInterval == 0 {
			elapsed := time.Since(lastLogTime)
			rate := float64(logInterval) / elapsed.Seconds()
			log.Debug().
				Int("prefixes_processed", count).
				Str("elapsed", humanfmt.Duration(time.Since(startTime))).
				Int("rate_per_sec", int(rate)).
				Msg("index builder: progress")
			lastLogTime = time.Now()
		}
		count++

		row, err := iter.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read from iterator: %w", err)
		}
		if err := b.Add(row); err != nil {
			return fmt.Errorf("add row: %w", err)
		}
	}

	log.Debug().
		Int("total_prefixes", count).
		Str("duration", humanfmt.Duration(time.Since(startTime))).
		Msg("index builder: AddAllWithContext complete")

	return nil
}

// Finalize closes all remaining stack nodes and writes final files.
func (b *IndexBuilder) Finalize() error {
	return b.FinalizeWithContext(context.Background())
}

// FinalizeWithContext closes all remaining stack nodes and writes final files with logging.
func (b *IndexBuilder) FinalizeWithContext(ctx context.Context) error {
	if b.closed {
		return nil
	}
	b.closed = true

	log := zerolog.Ctx(ctx)
	startTime := time.Now()

	log.Debug().Msg("index builder: starting Finalize")

	if err := b.closeNodesAbove(0); err != nil {
		return fmt.Errorf("close remaining nodes: %w", err)
	}

	log.Debug().
		Int("prefix_count", b.coreStatsW.Count()).
		Msg("index builder: finalizing core stats + tier stats files")

	if err := b.closeStreamingWriters(); err != nil {
		return err
	}

	log.Debug().Msg("index builder: building depth index")

	if err := b.depthIndexBuilder.Build(b.outDir); err != nil {
		return fmt.Errorf("build depth index: %w", err)
	}

	log.Debug().
		Uint64("prefix_count", b.mphfBuilder.Count()).
		Msg("index builder: building MPHF (this may take a while for large datasets)")

	if err := b.buildMPHF(ctx); err != nil {
		return err
	}

	if err := b.writeTierManifest(); err != nil {
		return err
	}

	log.Debug().Msg("index builder: writing manifest")

	if err := format.WriteManifest(b.outDir, b.posCount, b.maxDepth); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	log.Debug().Msg("index builder: syncing directory")

	if err := format.SyncDir(b.outDir); err != nil {
		return fmt.Errorf("sync dir: %w", err)
	}

	log.Debug().
		Str("total_duration", humanfmt.Duration(time.Since(startTime))).
		Msg("index builder: Finalize complete")

	return nil
}

// closeStreamingWriters finalizes the row-major core stats + tier
// stats files. Both leave their final files in place at the index
// dir; no rename / temp-promote dance.
func (b *IndexBuilder) closeStreamingWriters() error {
	if err := b.coreStatsW.Finalize(); err != nil {
		return fmt.Errorf("finalize core stats: %w", err)
	}
	b.coreStatsW = nil
	if err := b.tierStatsRowW.Close(); err != nil {
		return fmt.Errorf("close tier stats row: %w", err)
	}
	b.tierStatsRowW = nil

	return nil
}

// buildMPHF builds the MPHF and logs progress.
func (b *IndexBuilder) buildMPHF(ctx context.Context) error {
	log := zerolog.Ctx(ctx)
	mphfStart := time.Now()
	if err := b.mphfBuilder.Build(b.outDir); err != nil {
		return fmt.Errorf("build MPHF: %w", err)
	}
	b.mphfBuilder.Close()

	log.Debug().
		Str("mphf_duration", humanfmt.Duration(time.Since(mphfStart))).
		Msg("index builder: MPHF build complete")

	return nil
}

// writeTierManifest writes the tier manifest if tiers are present.
func (b *IndexBuilder) writeTierManifest() error {
	if len(b.presentTiers) == 0 {
		return nil
	}
	presentTierList := make([]tiers.ID, 0, len(b.presentTiers))
	for tierID := range b.presentTiers {
		presentTierList = append(presentTierList, tierID)
	}
	if err := tiers.WriteManifest(b.outDir, presentTierList); err != nil {
		return fmt.Errorf("write tier manifest: %w", err)
	}

	return nil
}

// Count returns the number of prefixes processed.
func (b *IndexBuilder) Count() uint64 {
	return b.posCount
}

// MaxDepth returns the maximum depth encountered.
func (b *IndexBuilder) MaxDepth() uint32 {
	return b.maxDepth
}
