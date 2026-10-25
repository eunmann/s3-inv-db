package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
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
	coreStatsW        *format.CoreStatsBuilder
	tierStatsW        *format.TierStatsWriter
	mphfBuilder       *format.StreamingMPHFBuilder
	prefixTreeW       *format.PrefixTreeWriter
	depthIndexBuilder *format.DepthIndexBuilder
	declaredTiers     []tiers.ID
	resolvedTiers     []tiers.ID
	outDir            string
	tempDir           string
	stack             []stackEntry
	posCount          uint64
	maxDepth          uint32
	tiersDeclared     bool
	closed            bool
	// createdOutDir is true when NewIndexBuilder created outDir, so
	// cleanup may remove it. A directory that already existed is never
	// removed: it may hold the caller's other files.
	createdOutDir bool
}

// stackEntry tracks an open prefix on the stack.
type stackEntry struct {
	prefix string
	pos    uint64
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
	_, statErr := os.Stat(outDir)
	createdOutDir := errors.Is(statErr, fs.ErrNotExist)
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

	prefixTreeW, err := format.NewPrefixTreeWriter(outDir)
	if err != nil {
		mphfBuilder.Close()
		coreStatsW.Close()

		return nil, fmt.Errorf("create prefix tree writer: %w", err)
	}

	b := &IndexBuilder{
		outDir:            outDir,
		tempDir:           tempDir,
		coreStatsW:        coreStatsW,
		mphfBuilder:       mphfBuilder,
		prefixTreeW:       prefixTreeW,
		depthIndexBuilder: format.NewDepthIndexBuilder(tempDir),
		stack:             make([]stackEntry, 0, 32),
		createdOutDir:     createdOutDir,
	}

	// The tier-stats writer is created lazily on the first Add, once
	// the present-tier set is fixed (default or via SetPresentTiers).

	return b, nil
}

// ErrPresentTiersAfterAdd is returned when SetPresentTiers is called
// after the first Add — the tier slots are fixed when the writer is
// created on the first Add and cannot change mid-stream.
var ErrPresentTiersAfterAdd = errors.New("SetPresentTiers must be called before the first Add")

// SetPresentTiers declares which tiers carry data, so only they get a
// tier slot. The set comes from the ingest tier mask. When unset, the
// builder gives every tier a slot. Must be called before the first Add.
func (b *IndexBuilder) SetPresentTiers(present []tiers.ID) error {
	if b.posCount > 0 {
		return ErrPresentTiersAfterAdd
	}
	b.declaredTiers = slices.Compact(slices.Sorted(slices.Values(present)))
	b.tiersDeclared = true

	return nil
}

// ensureTierWriter lazily creates the tier-stats writer on the first
// Add, resolving the present-tier set (declared, else every tier).
func (b *IndexBuilder) ensureTierWriter() error {
	if b.tierStatsW != nil {
		return nil
	}
	present := b.declaredTiers
	if !b.tiersDeclared {
		present = tiers.AllTierIDs()
	}
	w, err := format.NewTierStatsWriter(b.outDir, present)
	if err != nil {
		return fmt.Errorf("create tier stats writer: %w", err)
	}
	b.tierStatsW = w
	b.resolvedTiers = present

	return nil
}

// cleanup releases every writer and scratch file after a failed build,
// and removes outDir if this builder created it. Safe after a partial
// Finalize: each writer is nil-guarded or idempotent.
func (b *IndexBuilder) cleanup() {
	if b.coreStatsW != nil {
		b.coreStatsW.Close()
	}
	if b.mphfBuilder != nil {
		b.mphfBuilder.Close()
	}
	if b.prefixTreeW != nil {
		_ = b.prefixTreeW.Abort()
	}
	if b.tierStatsW != nil {
		_ = b.tierStatsW.Abort()
	}
	if b.depthIndexBuilder != nil {
		b.depthIndexBuilder.Close()
	}
	if b.createdOutDir {
		os.RemoveAll(b.outDir)
	}
}

// ErrMissingParent is returned when a prefix arrives without its
// parent before it: the root "" first, then every "a/b/" after "a/".
// The aggregator always emits every ancestor; the index stores each
// prefix as its parent plus one segment.
var ErrMissingParent = errors.New("prefix added without its parent")

// Add processes a single PrefixRow from the sorted stream.
// Prefixes must be added in lexicographic (sorted) order, the root ""
// first and every other prefix after its parent.
func (b *IndexBuilder) Add(row *PrefixRow) error {
	commonDepth := b.findCommonAncestorDepth(row.Prefix)
	if err := b.closeNodesAbove(commonDepth); err != nil {
		return err
	}
	if err := b.addToPrefixTree(row.Prefix); err != nil {
		return err
	}

	pos := b.posCount
	b.posCount++

	b.stack = append(b.stack, stackEntry{prefix: row.Prefix, pos: pos})

	if uint32(row.Depth) > b.maxDepth {
		b.maxDepth = uint32(row.Depth)
	}

	if err := b.depthIndexBuilder.Add(pos, uint32(row.Depth)); err != nil {
		return fmt.Errorf("add to depth index: %w", err)
	}
	if err := b.mphfBuilder.Add(row.Prefix, pos); err != nil {
		return fmt.Errorf("add to MPHF builder: %w", err)
	}

	if err := b.ensureTierWriter(); err != nil {
		return err
	}
	mask, err := b.tierStatsW.Add(&row.TierCounts, &row.TierBytes, row.Count, row.TotalBytes)
	if err != nil {
		return fmt.Errorf("prefix %q: write tier stats: %w", row.Prefix, err)
	}
	if err := b.coreStatsW.Add(row.Count, row.TotalBytes, row.Depth, mask); err != nil {
		return fmt.Errorf("write core stats row: %w", err)
	}

	return nil
}

// addToPrefixTree adds prefix as the open prefix at the top of the
// stack plus one segment and a slash, or as the root when it comes
// first.
func (b *IndexBuilder) addToPrefixTree(prefix string) error {
	if len(b.stack) == 0 {
		if prefix != "" || b.posCount != 0 {
			return fmt.Errorf("%w: %q", ErrMissingParent, prefix)
		}

		return b.prefixTreeW.AddRoot() //nolint:wrapcheck // the writer's errors carry their own context
	}
	parent := b.stack[len(b.stack)-1]
	name, ok := strings.CutSuffix(strings.TrimPrefix(prefix, parent.prefix), "/")
	if !ok || strings.Contains(name, "/") {
		return fmt.Errorf("%w: %q after %q", ErrMissingParent, prefix, parent.prefix)
	}
	if err := b.prefixTreeW.Add(parent.pos, name); err != nil {
		return fmt.Errorf("prefix %q: %w", prefix, err)
	}

	return nil
}

// findCommonAncestorDepth finds the depth of the deepest common ancestor.
func (b *IndexBuilder) findCommonAncestorDepth(prefix string) int {
	commonDepth := 0

	for i := range len(b.stack) {
		entry := &b.stack[i]
		if strings.HasPrefix(prefix, entry.prefix) {
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

	if err := b.coreStatsW.SetSubtree(top.pos, b.posCount-1); err != nil {
		return fmt.Errorf("set subtree_end: %w", err)
	}

	return nil
}

// AddAll processes all PrefixRows from an iterator, checking ctx for
// cancellation periodically.
func (b *IndexBuilder) AddAll(ctx context.Context, iter RowIterator) error {
	const checkInterval = 1000 // Check context every N rows
	const logInterval = 100000 // Log progress every N rows
	log := zerolog.Ctx(ctx)
	count := 0
	startTime := time.Now()
	lastLogTime := startTime

	log.Debug().Msg("index builder: starting AddAll")

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
		Msg("index builder: AddAll complete")

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

// closeStreamingWriters finalizes the row-major core stats and tier
// stats files in the index dir.
func (b *IndexBuilder) closeStreamingWriters() error {
	if err := b.coreStatsW.Finalize(); err != nil {
		return fmt.Errorf("finalize core stats: %w", err)
	}
	b.coreStatsW = nil
	if err := b.prefixTreeW.Close(); err != nil {
		return fmt.Errorf("finalize prefix tree: %w", err)
	}
	b.prefixTreeW = nil
	// The tier writer is nil when no rows were added (empty index); such
	// a build writes no tier file and no tier manifest (tiers.json). The
	// index manifest is still written unconditionally in Finalize.
	if b.tierStatsW != nil {
		if err := b.tierStatsW.Close(); err != nil {
			return fmt.Errorf("close tier stats: %w", err)
		}
		b.tierStatsW = nil
	}

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

// writeTierManifest writes the tier manifest listing the resolved
// present tiers, in the same slot order the row writer used. Skipped
// for an empty index (no rows, no tier writer), matching the reader's
// "no manifest → empty tier reader" contract.
func (b *IndexBuilder) writeTierManifest() error {
	if b.posCount == 0 || len(b.resolvedTiers) == 0 {
		return nil
	}
	if err := tiers.WriteManifest(b.outDir, b.resolvedTiers); err != nil {
		return fmt.Errorf("write tier manifest: %w", err)
	}

	return nil
}

func (b *IndexBuilder) Count() uint64 {
	return b.posCount
}

func (b *IndexBuilder) MaxDepth() uint32 {
	return b.maxDepth
}
