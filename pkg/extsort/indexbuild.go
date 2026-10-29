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
	"sync"
	"time"
	"unsafe"

	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// IndexBuilder builds index files from a sorted stream of PrefixRows.
//
// Rows arrive in key order (preorder); the index numbers them in level
// order: the root, every depth-1 prefix, every depth-2 prefix, each
// depth in key order. A depth's rows are then contiguous, so a
// prefix's children are the rows from its child start to the next
// row's, and scans of a level read its rows in sequence. Add checks
// each row against its ancestors (a depth stack) and spools it by
// depth (levelSpool); Finalize replays the depths in order into the
// index writers.
//
// Memory is bounded: rows and prefix hashes go to disk, and the stack
// holds one entry per depth.
type IndexBuilder struct {
	coreStatsW    *format.CoreStatsBuilder
	tierStatsW    *format.TierStatsWriter
	mphfBuilder   *format.StreamingMPHFBuilder
	prefixTreeW   *format.PrefixTreeWriter
	spool         *levelSpool
	levelRows     []uint64 // rows seen so far at each depth
	declaredTiers []tiers.ID
	resolvedTiers []tiers.ID
	outDir        string
	tempDir       string
	stack         []stackEntry
	posCount      uint64
	maxDepth      uint32
	// onProgress receives Finalize's steps (SetProgress); stepProgress
	// is the step reporter Finalize hands its writers.
	onProgress    events.ProgressFunc
	stepProgress  format.StepProgress
	tiersDeclared bool
	closed        bool
	// createdOutDir is true when NewIndexBuilder created outDir, so
	// cleanup may remove it. A directory that already existed is never
	// removed: it may hold the caller's other files.
	createdOutDir bool
}

// stackEntry tracks an open prefix on the stack: its index within its
// depth.
type stackEntry struct {
	prefix string
	local  uint64
}

// NewIndexBuilder creates a streaming index builder.
// The tempDir is used for temporary storage during construction (for MPHF builder).
// If tempDir is empty, os.TempDir() is used.
func NewIndexBuilder(outDir, tempDir string) (*IndexBuilder, error) {
	return NewIndexBuilderWithCapacity(outDir, tempDir, 0)
}

// NewIndexBuilderWithCapacity creates a streaming index builder with a capacity hint.
// The capacityHint, an upper bound on the prefix count, pre-sizes the core
// stats build file (capped by format.NewCoreStatsBuilder). If capacityHint
// is 0, a small default capacity is used.
func NewIndexBuilderWithCapacity(outDir, tempDir string, capacityHint uint64) (*IndexBuilder, error) {
	_, statErr := os.Stat(outDir)
	createdOutDir := errors.Is(statErr, fs.ErrNotExist)
	if err := os.MkdirAll(outDir, format.DirPerm); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	if tempDir == "" {
		tempDir = os.TempDir()
	}

	spoolDir, err := os.MkdirTemp(tempDir, "level-spool-")
	if err != nil {
		return nil, fmt.Errorf("create level spool dir: %w", err)
	}
	mphfBuilder, err := format.NewStreamingMPHFBuilder(tempDir)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create MPHF builder: %w", err), os.RemoveAll(spoolDir))
	}

	arrayCap := max(capacityHint, uint64(1024))

	coreStatsW, err := format.NewCoreStatsBuilder(outDir, arrayCap)
	if err != nil {
		mphfBuilder.Close()

		return nil, errors.Join(fmt.Errorf("create core stats builder: %w", err), os.RemoveAll(spoolDir))
	}

	prefixTreeW, err := format.NewPrefixTreeWriter(outDir)
	if err != nil {
		mphfBuilder.Close()
		coreStatsW.Close()

		return nil, errors.Join(fmt.Errorf("create prefix tree writer: %w", err), os.RemoveAll(spoolDir))
	}

	b := &IndexBuilder{
		outDir:        outDir,
		tempDir:       tempDir,
		coreStatsW:    coreStatsW,
		mphfBuilder:   mphfBuilder,
		prefixTreeW:   prefixTreeW,
		spool:         newLevelSpool(spoolDir),
		stack:         make([]stackEntry, 0, 32),
		createdOutDir: createdOutDir,
	}

	// The tier-stats writer is created at Finalize, when the rows are
	// replayed; the present-tier set it uses is frozen at the first Add.

	return b, nil
}

// ErrPresentTiersAfterAdd is returned when SetPresentTiers is called
// after the first Add: the tier set is frozen there, and the tier
// writer Finalize creates gives slots to exactly that set.
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

// ensureTierWriter creates the tier-stats writer at Finalize, resolving
// the present-tier set (declared, else every tier).
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
	if b.spool != nil {
		_ = b.spool.close()
		_ = os.RemoveAll(b.spool.dir)
	}
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
	if b.createdOutDir {
		os.RemoveAll(b.outDir)
	}
}

// ErrMissingParent is returned when a prefix arrives without its
// parent before it: the root "" first, then every "a/b/" after "a/".
// The aggregator always emits every ancestor; the index stores each
// prefix as its parent plus one segment.
var ErrMissingParent = errors.New("prefix added without its parent")

// ErrDepthMismatch is returned when a row's Depth disagrees with the
// depth its parent chain gives it.
var ErrDepthMismatch = errors.New("prefix depth disagrees with its parent chain")

// ErrUnsortedInput is returned when a prefix doesn't sort strictly
// after the one before it.
var ErrUnsortedInput = errors.New("prefix out of order")

// Add processes a single PrefixRow from the sorted stream.
// Prefixes must be added in lexicographic (sorted) order, the root ""
// first and every other prefix after its parent.
func (b *IndexBuilder) Add(row *PrefixRow) error {
	// The previous row is still on top of the stack. Readers rely on
	// siblings sitting in byte order of their full prefixes (compare
	// merges two indexes' children by it), so check it rather than
	// trust the input.
	if n := len(b.stack); n > 0 && row.Prefix <= b.stack[n-1].prefix {
		return fmt.Errorf("%w: %q after %q", ErrUnsortedInput, row.Prefix, b.stack[n-1].prefix)
	}
	commonDepth := b.findCommonAncestorDepth(row.Prefix)
	b.closeNodesAbove(commonDepth)
	name, err := b.segmentOf(row.Prefix)
	if err != nil {
		return err
	}
	// The parent check proves the prefix's depth is the stack depth; a
	// row that says otherwise would land in the wrong level.
	if int(row.Depth) != len(b.stack) {
		return fmt.Errorf("%w: %q has depth %d, its place in the tree says %d",
			ErrDepthMismatch, row.Prefix, row.Depth, len(b.stack))
	}

	depth := len(b.stack)
	for len(b.levelRows) <= depth+1 {
		b.levelRows = append(b.levelRows, 0)
	}
	e := b.spool.next()
	e.depth = depth
	r := &e.rec
	r.count, r.bytes = row.Count, row.TotalBytes
	// Every row of the next depth so far belongs to an earlier prefix,
	// so this prefix's children start where they end.
	r.childLocal = b.levelRows[depth+1]
	r.parentLocal = 0
	if depth > 0 {
		r.parentLocal = b.stack[depth-1].local
	}
	// The prefix is still in this core's cache from the checks above:
	// hashed by the spool writer instead, it cost twice as much.
	r.keyHash, r.fp = b.mphfBuilder.KeyHashes(row.Prefix)
	r.name = append(r.name[:0], name...)
	r.tierCounts, r.tierBytes = row.TierCounts, row.TierBytes
	if err := b.spool.commit(); err != nil {
		return err
	}

	b.stack = append(b.stack, stackEntry{prefix: row.Prefix, local: b.levelRows[depth]})
	b.levelRows[depth]++
	b.posCount++
	b.maxDepth = max(b.maxDepth, uint32(depth))

	return nil
}

// segmentOf returns prefix's last segment, without its slash: prefix
// must be the open prefix at the top of the stack plus that segment
// and a slash, or the root when it comes first.
func (b *IndexBuilder) segmentOf(prefix string) (string, error) {
	if len(b.stack) == 0 {
		if prefix != "" || b.posCount != 0 {
			return "", fmt.Errorf("%w: %q", ErrMissingParent, prefix)
		}

		return "", nil
	}
	parent := b.stack[len(b.stack)-1]
	name, ok := strings.CutSuffix(strings.TrimPrefix(prefix, parent.prefix), "/")
	if !ok || strings.Contains(name, "/") {
		return "", fmt.Errorf("%w: %q after %q", ErrMissingParent, prefix, parent.prefix)
	}

	return name, nil
}

// findCommonAncestorDepth finds the depth of the deepest common
// ancestor. The stack is a chain of ancestors, so the entries that are
// prefixes of the new row form its bottom run: scanning from the top
// stops at the first match, about one comparison per row amortized.
func (b *IndexBuilder) findCommonAncestorDepth(prefix string) int {
	for i, v := range slices.Backward(b.stack) {
		if strings.HasPrefix(prefix, v.prefix) {
			return i + 1
		}
	}

	return 0
}

// closeNodesAbove closes all stack nodes with depth > targetDepth.
func (b *IndexBuilder) closeNodesAbove(targetDepth int) {
	b.stack = b.stack[:min(len(b.stack), targetDepth)]
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

// SetProgress makes Finalize report its steps, and each step's
// progress, as the finalizing stage's. Call it before Finalize.
func (b *IndexBuilder) SetProgress(fn events.ProgressFunc) { b.onProgress = fn }

// FinalizeWithContext closes all remaining stack nodes and writes final
// files. Each step is reported to the SetProgress callback and logged
// at info level with its duration and storage waits.
func (b *IndexBuilder) FinalizeWithContext(ctx context.Context) error {
	if b.closed {
		return nil
	}
	b.closed = true

	log := zerolog.Ctx(ctx)
	startTime := time.Now()
	steps := newFinalizeSteps(log, b.onProgress)
	defer steps.end()
	b.stepProgress = steps.report
	b.mphfBuilder.SetProgress(b.stepProgress)

	log.Debug().Msg("index builder: starting Finalize")

	b.closeNodesAbove(0)

	log.Debug().Uint64("prefix_count", b.posCount).Msg("index builder: writing rows in level order")

	if err := b.writeLevels(); err != nil {
		return err
	}

	log.Debug().
		Int("prefix_count", b.coreStatsW.Count()).
		Uint64("mphf_keys", b.mphfBuilder.Count()).
		Msg("index builder: finalizing row files and building the MPHF")

	// The row files' repacks and the MPHF build share nothing, so they
	// run side by side: the build's serial tail is the longer of the two.
	rowsDone := make(chan error, 1)
	go func() { rowsDone <- b.closeStreamingWriters() }()
	mphfErr := b.buildMPHF(ctx)
	b.stepProgress(stepFinishRows, 0, 1)
	if err := errors.Join(<-rowsDone, mphfErr); err != nil {
		return err
	}

	if err := b.writeTierManifest(); err != nil {
		return err
	}
	b.stepProgress(stepFinishRows, 1, 1)

	log.Debug().Msg("index builder: writing manifest")

	if err := format.WriteManifest(b.outDir, b.posCount, b.maxDepth, b.stepProgress); err != nil {
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

// levelsProgressEvery is how many rows writeLevels writes between
// progress reports: a power of two, so the check is a mask.
const levelsProgressEvery = 1 << 17

// writeLevels replays the spooled rows depth by depth into the index
// writers, so every per-row file is in level order. A row's child start
// and parent become rows once every depth's size is known: the first
// row of a depth is the number of rows above it.
func (b *IndexBuilder) writeLevels() error {
	if err := b.spool.finish(); err != nil {
		return err
	}
	depthW, err := format.NewDepthIndexWriter(b.outDir)
	if err != nil {
		return fmt.Errorf("create depth index: %w", err)
	}
	starts := make([]uint64, len(b.levelRows)+1)
	for d, n := range b.levelRows {
		starts[d+1] = starts[d] + n
	}
	if b.posCount > 0 {
		if err := b.ensureTierWriter(); err != nil {
			return errors.Join(err, depthW.Abort())
		}
	}
	total := int64(b.posCount)
	var written int64
	b.stepProgress(stepWriteLevels, 0, total)
	for depth := range len(b.spool.depths) {
		// Two groups of writers share nothing (stats rows on one side,
		// names and MPHF keys on the other), so each reads the depth's
		// records itself and they write side by side. The stats side
		// counts the rows.
		names := make(chan error, 1)
		go func() {
			names <- b.spool.read(depth, func(r *spoolRecord) error { return b.writeNameRow(starts, depth, r) })
		}()
		err := b.spool.read(depth, func(r *spoolRecord) error {
			written++
			if written%levelsProgressEvery == 0 {
				b.stepProgress(stepWriteLevels, written, total)
			}

			return b.writeStatsRow(depthW, starts, depth, r)
		})
		if err = errors.Join(err, <-names); err == nil {
			err = b.spool.remove(depth)
		}
		if err != nil {
			return errors.Join(err, depthW.Abort())
		}
	}
	if err := depthW.Close(); err != nil {
		return fmt.Errorf("write depth index: %w", err)
	}
	b.stepProgress(stepWriteLevels, total, total)

	return os.RemoveAll(b.spool.dir) //nolint:wrapcheck // the path is in the error
}

// writeStatsRow writes one spooled row's stats, the next in level
// order: its tier stats, core row and depth-index entry.
func (b *IndexBuilder) writeStatsRow(depthW *format.DepthIndexWriter, starts []uint64, depth int, r *spoolRecord) error {
	mask, err := b.tierStatsW.Add(&r.tierCounts, &r.tierBytes, r.count, r.bytes)
	if err != nil {
		return fmt.Errorf("prefix segment %q at depth %d: write tier stats: %w", r.name, depth, err)
	}
	if err := b.coreStatsW.Add(r.count, r.bytes, starts[depth+1]+r.childLocal, mask); err != nil {
		return fmt.Errorf("write core stats row: %w", err)
	}
	if err := depthW.Add(uint32(depth), r.count, r.bytes); err != nil {
		return fmt.Errorf("add to depth index: %w", err)
	}

	return nil
}

// writeNameRow writes one spooled row's name, the next in level order:
// its prefix-tree row and MPHF key.
func (b *IndexBuilder) writeNameRow(starts []uint64, depth int, r *spoolRecord) error {
	var err error
	if depth == 0 {
		err = b.prefixTreeW.AddRoot()
	} else {
		// The writer allows a name that aliases a reused buffer.
		err = b.prefixTreeW.Add(starts[depth-1]+r.parentLocal, unsafe.String(unsafe.SliceData(r.name), len(r.name)))
	}
	if err != nil {
		return fmt.Errorf("prefix segment %q at depth %d: %w", r.name, depth, err)
	}
	if err := b.mphfBuilder.AddHashed(r.keyHash, r.fp); err != nil {
		return fmt.Errorf("add to MPHF builder: %w", err)
	}

	return nil
}

// closeStreamingWriters finalizes the row-major core stats, prefix tree
// and tier stats files in the index dir. Each rewrites its own file from
// its own scratch through bounded buffers, so they run side by side; a
// writer whose close fails stays set for cleanup to abort.
func (b *IndexBuilder) closeStreamingWriters() error {
	if b.tierStatsW != nil {
		b.coreStatsW.SetTierMaskEncoding(b.tierStatsW.MaskEncoding())
	}
	var coreErr, treeErr, tierErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		if coreErr = b.coreStatsW.Finalize(); coreErr != nil {
			coreErr = fmt.Errorf("finalize core stats: %w", coreErr)
		} else {
			b.coreStatsW = nil
		}
	})
	wg.Go(func() {
		if treeErr = b.prefixTreeW.Close(); treeErr != nil {
			treeErr = fmt.Errorf("finalize prefix tree: %w", treeErr)
		} else {
			b.prefixTreeW = nil
		}
	})
	// The tier writer is nil when no rows were added (empty index); such
	// a build writes no tier file and no tier manifest (tiers.json). The
	// index manifest is still written unconditionally in Finalize.
	if b.tierStatsW != nil {
		wg.Go(func() {
			if tierErr = b.tierStatsW.Close(); tierErr != nil {
				tierErr = fmt.Errorf("close tier stats: %w", tierErr)
			} else {
				b.tierStatsW = nil
			}
		})
	}
	wg.Wait()

	return errors.Join(coreErr, treeErr, tierErr)
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
