package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
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
// depth (levelSpool), whose stages also plan the core and tier files'
// widths from every row, intern its last segment and spill its MPHF key;
// Finalize replays the depths in order into the index writers, which
// write each file in its final form.
//
// Memory is bounded: rows and MPHF keys go to disk, and the stack
// holds one entry per depth.
type IndexBuilder struct {
	// The plans and the interner are filled on the spool's plan stage,
	// and read once Finalize has stopped it.
	tierPlan *format.TierStatsPlan
	corePlan format.CoreStatsPlan
	interner *format.PrefixSegmentInterner
	// treePlan holds, per depth, the rows the spool's plan has seen
	// and the most a row's index within its depth exceeds its parent's:
	// with the depths' sizes that gives the widest parent distance.
	treePlan treePlan
	coreW    *format.CoreStatsWriter
	tierW    *format.TierStatsWriter
	// mphfBuilder takes the keys on the spool's keys stage.
	mphfBuilder *format.StreamingMPHFBuilder
	prefixTreeW *format.PrefixTreeWriter
	// checksums collects the manifest checksums the writers take of
	// their files as they write them.
	checksums     *format.Checksums
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
	onProgress   events.ProgressFunc
	stepProgress format.StepProgress
	// replayWorkers is the goroutines writeLevels replays chunks on; 0
	// is GOMAXPROCS.
	replayWorkers int
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

// NewIndexBuilder creates a streaming index builder. The tempDir holds
// its scratch (the level spool and the MPHF builder's spill files); if it is
// empty, os.TempDir() is used.
func NewIndexBuilder(outDir, tempDir string) (*IndexBuilder, error) {
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
	mphfBuilder := format.NewStreamingMPHFBuilder(tempDir)
	mphfBuilder.SetMemoryBudget(mphfMemoryBudget(readMemoryLimit()))

	checksums := format.NewChecksums()
	interner, err := format.NewPrefixSegmentInterner(outDir, checksums)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create prefix segment interner: %w", err), os.RemoveAll(spoolDir))
	}

	b := &IndexBuilder{
		outDir:        outDir,
		tempDir:       tempDir,
		mphfBuilder:   mphfBuilder,
		checksums:     checksums,
		interner:      interner,
		spool:         newLevelSpool(spoolDir),
		stack:         make([]stackEntry, 0, 32),
		createdOutDir: createdOutDir,
	}
	b.spool.plan, b.spool.keys = b.planRow, b.addKey

	return b, nil
}

// ErrPresentTiersAfterAdd is returned when SetPresentTiers is called
// after the first Add: the tier set is frozen there, where the tier
// plan gives slots to exactly that set.
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

// startTierPlan starts the tier plan at the first Add, resolving the
// present-tier set (declared, else every tier).
func (b *IndexBuilder) startTierPlan() error {
	present := b.declaredTiers
	if !b.tiersDeclared {
		present = tiers.AllTierIDs()
	}
	plan, err := format.NewTierStatsPlan(present)
	if err != nil {
		return fmt.Errorf("plan tier stats: %w", err)
	}
	b.tierPlan, b.resolvedTiers = plan, present

	return nil
}

// planRow adds a row to the core, tier and tree plans, gives its record
// its tier slots, appended to slots, and interns its last segment into
// the record's segment ID. It runs on the spool's plan stage, which
// sees every row once, in Add order.
func (b *IndexBuilder) planRow(e *spoolEntry, slots []uint64) ([]uint64, error) {
	r, depth := &e.rec, e.depth
	start := len(slots)
	mask, slots, err := b.tierPlan.Add(&e.tierCounts, &e.tierBytes, r.count, r.bytes, slots)
	if err != nil {
		return slots, fmt.Errorf("prefix segment %q at depth %d: tier stats: %w", e.name, depth, err)
	}
	r.tierMask, r.tiers = mask, slots[start:]
	b.corePlan.Add(r.count, r.bytes, mask)
	if depth > 0 {
		// The interner copies a new segment, so the name may alias the
		// entry's buffer.
		if r.segID, err = b.interner.Intern(unsafe.String(unsafe.SliceData(e.name), len(e.name))); err != nil {
			return slots, fmt.Errorf("prefix segment %q at depth %d: %w", e.name, depth, err)
		}
	}
	b.treePlan.add(depth, r.parentLocal)

	return slots, nil
}

// addKey adds a row's key to the MPHF builder. It runs on the spool's
// keys stage, which sees every row once, in Add order, beside the
// stage writing the row's record: it only reads the entry.
func (b *IndexBuilder) addKey(e *spoolEntry) error {
	posKey, err := format.PositionKey(e.depth, e.local)
	if err == nil {
		err = b.mphfBuilder.AddKey(e.keyHash, e.fp, posKey)
	}
	if err != nil {
		return fmt.Errorf("prefix segment %q at depth %d: add to MPHF builder: %w", e.name, e.depth, err)
	}

	return nil
}

// treePlan follows each depth's rows on the spool's plan stage.
// A row's parent distance in level order is its depth's start minus its
// parent's depth's start (the parent depth's size) plus its index within
// its depth minus its parent's; the plan keeps the largest second part
// per depth.
type treePlan struct {
	rows     []uint64 // rows seen at each depth
	maxDelta []int64  // the largest index minus parent index at each depth
}

func (p *treePlan) add(depth int, parentLocal uint64) {
	for len(p.rows) <= depth {
		p.rows = append(p.rows, 0)
		p.maxDelta = append(p.maxDelta, math.MinInt64)
	}
	p.maxDelta[depth] = max(p.maxDelta[depth], int64(p.rows[depth])-int64(parentLocal))
	p.rows[depth]++
}

// maxDist is the widest parent distance once every depth's size is
// known; 0 when only the root has been added.
func (p *treePlan) maxDist(levelRows []uint64) uint64 {
	var widest int64
	for depth := 1; depth < len(p.maxDelta); depth++ {
		widest = max(widest, int64(levelRows[depth-1])+p.maxDelta[depth])
	}

	return uint64(widest)
}

// cleanup releases every writer and scratch file after a failed build,
// and removes outDir if this builder created it. Safe after a partial
// Finalize: each writer is nil-guarded or idempotent.
func (b *IndexBuilder) cleanup() {
	if b.spool != nil {
		_ = b.spool.close()
		_ = os.RemoveAll(b.spool.dir)
	}
	if b.coreW != nil {
		_ = b.coreW.Abort()
	}
	if b.mphfBuilder != nil {
		b.mphfBuilder.Close()
	}
	if b.interner != nil {
		_ = b.interner.Close()
	}
	if b.prefixTreeW != nil {
		_ = b.prefixTreeW.Abort()
	}
	if b.tierW != nil {
		_ = b.tierW.Abort()
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
// after the one before it, or, in a run being merged, sorts before it.
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

	if b.posCount == 0 {
		if err := b.startTierPlan(); err != nil {
			return err
		}
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
	// hashed on a spool stage instead, it cost twice as much.
	e.keyHash, e.fp = b.mphfBuilder.KeyHashes(row.Prefix)
	e.local = b.levelRows[depth]
	e.name = append(e.name[:0], name...)
	e.tierCounts, e.tierBytes = row.TierCounts, row.TierBytes
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
// at info level with its duration and storage waits. The files are not
// synced: publishing the staged index makes them durable in one pass.
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

	log.Debug().Msg("index builder: starting Finalize")

	b.closeNodesAbove(0)

	segments, err := b.finishSpool()
	if err != nil {
		return err
	}
	starts := b.levelStarts()

	log.Debug().
		Uint64("mphf_keys", b.mphfBuilder.Count()).
		Msg("index builder: building the MPHF beside the level-order rows")

	mphfStart := time.Now()
	mphf, err := b.mphfBuilder.Start(ctx, b.outDir, starts, max(1, runtime.GOMAXPROCS(0)-writeLevelsProcs), b.checksums)
	if err != nil {
		return fmt.Errorf("build MPHF: %w", err)
	}

	log.Debug().Uint64("prefix_count", b.posCount).Msg("index builder: writing rows in level order")

	busy, err := b.writeLevels(ctx, starts, segments)
	if err == nil {
		steps.annotate(stepWriteLevels, busy.fields)
		err = b.writeTierManifest()
	}
	if err != nil {
		mphf.Abort()

		return errors.Join(err, mphf.Wait())
	}
	if err := b.finishMPHF(ctx, mphf, mphfStart); err != nil {
		return err
	}

	log.Debug().Msg("index builder: writing manifest")

	reread, err := format.WriteManifest(b.outDir, b.posCount, b.maxDepth, b.checksums, b.stepProgress)
	if err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	log.Debug().
		Int("files_reread", len(reread)).
		Strs("reread", reread).
		Msg("index builder: manifest checksums taken while writing, the rest read back")

	log.Debug().
		Str("total_duration", humanfmt.Duration(time.Since(startTime))).
		Msg("index builder: Finalize complete")

	return nil
}

// levelsProgressEvery is how many rows writeLevels writes between
// progress reports: a power of two, so the check is a mask.
const levelsProgressEvery = 1 << 17

// replayBusy is how long the replay took, on how many workers, how long
// they worked in all (the cores it kept busy are work/wall) and how long
// merging their chunks took, without its waits on them.
type replayBusy struct {
	wall, work, merge time.Duration
	workers           int
}

func (b replayBusy) fields(e *zerolog.Event) *zerolog.Event {
	return e.Dur("replay_ms", b.wall).
		Int("replay_workers", b.workers).
		Dur("work_busy_ms", b.work).
		Dur("merge_busy_ms", b.merge)
}

// writeLevels replays the spooled rows depth by depth into the index
// writers, so every per-row file is in level order. A row's child start
// and parent become rows once every depth's size is known: the first
// row of a depth is the number of rows above it. The writers take their
// widths from the plans the spool's plan stage filled.
//
// The rows replay in chunks on every core (replayChunks), unless the
// build has fewer than two chunks of rows or one core: then there is
// nothing to split, and this goroutine replays them through the
// sequential writers, which also serve as the range writers' oracle in
// tests.
func (b *IndexBuilder) writeLevels(ctx context.Context, starts []uint64, segments uint32) (replayBusy, error) {
	total := int64(b.posCount)
	b.stepProgress(stepWriteLevels, 0, total)
	if err := b.planChildStartTail(starts); err != nil {
		return replayBusy{}, err
	}
	workers := b.replayWorkers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	var busy replayBusy
	var err error
	if workers < 2 || b.posCount < 2*b.spool.markEvery {
		busy, err = b.replaySequential(ctx, starts, segments)
	} else {
		busy, err = b.replayParallel(ctx, starts, segments, workers)
	}
	if err != nil {
		return busy, err
	}
	b.stepProgress(stepWriteLevels, total, total)

	return busy, os.RemoveAll(b.spool.dir) //nolint:wrapcheck // the path is in the error
}

// replayParallel replays the spooled rows in chunks on workers
// goroutines into the range writers.
func (b *IndexBuilder) replayParallel(ctx context.Context, starts []uint64, segments uint32, workers int) (replayBusy, error) {
	levels, err := b.openLevelRanges(segments)
	if err != nil {
		return replayBusy{}, err
	}
	busy, err := b.replayChunks(ctx, levels, starts, b.spool.chunks(), workers)
	if err != nil {
		return busy, errors.Join(err, levels.abort())
	}

	return busy, levels.close()
}

// replaySequential replays the spooled rows on this goroutine into the
// sequential writers, each depth's file removed once read.
func (b *IndexBuilder) replaySequential(ctx context.Context, starts []uint64, segments uint32) (replayBusy, error) {
	start := time.Now()
	depthW, err := b.openStatsWriters()
	if err != nil {
		return replayBusy{}, err
	}
	if b.prefixTreeW, err = format.NewPrefixTreeWriter(b.outDir, b.posCount, segments, b.treePlan.maxDist(b.levelRows), b.checksums); err != nil {
		return replayBusy{}, errors.Join(fmt.Errorf("create prefix tree: %w", err), depthW.Abort())
	}
	if err := b.replayDepths(ctx, depthW, starts); err != nil {
		return replayBusy{}, errors.Join(err, depthW.Abort())
	}
	if err := b.closeStatsWriters(depthW); err != nil {
		return replayBusy{}, err
	}
	if err := b.prefixTreeW.Close(); err != nil {
		return replayBusy{}, fmt.Errorf("close prefix tree: %w", err)
	}
	b.prefixTreeW = nil
	wall := time.Since(start)

	return replayBusy{wall: wall, work: wall, workers: 1}, nil
}

// replayDepths writes every depth's rows in order to the sequential
// writers.
func (b *IndexBuilder) replayDepths(ctx context.Context, depthW *format.DepthIndexWriter, starts []uint64) error {
	total := int64(b.posCount)
	var row int64
	var rec spoolRecord
	var vals []uint64
	for depth := range len(b.spool.depths) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("index build cancelled: %w", err)
		}
		r, err := b.spool.open(depth)
		if err != nil {
			return err
		}
		for err == nil {
			if vals, err = r.next(&rec, vals[:0]); err != nil {
				break
			}
			if err = b.writeStatsRow(depthW, starts, row, depth, &rec); err == nil {
				err = b.writeNameRow(starts, depth, &rec)
			}
			row++
			if row%levelsProgressEvery == 0 {
				b.stepProgress(stepWriteLevels, row, total)
			}
		}
		if err := errors.Join(ignoreEOF(err), r.close()); err != nil {
			return err
		}
		if err := b.spool.remove(depth); err != nil {
			return err
		}
	}

	return nil
}

// finishSpool ends the spool's stages, which completes the plans and
// the MPHF keys, and closes the interner, which writes the dictionary's
// end and frees its segment map before the replay. It returns the
// segment count.
func (b *IndexBuilder) finishSpool() (uint32, error) {
	if err := b.spool.finish(); err != nil {
		return 0, err
	}
	err := b.interner.Close()
	segments := b.interner.Count()
	b.interner = nil
	if err != nil {
		return 0, fmt.Errorf("write prefix dictionary: %w", err)
	}

	return segments, nil
}

// levelStarts returns each depth's first row in level order and, last,
// the row count.
func (b *IndexBuilder) levelStarts() []uint64 {
	starts := make([]uint64, len(b.levelRows)+1)
	for d, n := range b.levelRows {
		starts[d+1] = starts[d] + n
	}

	return starts
}

// planChildStartTail gives the core plan, when it needs them, the rows
// whose child start is at least ChildStartTail's bound: a run at the end
// of the level order, so it reads the depths from the deepest up and
// stops at the first whose child starts all fall short. Depth d's child
// starts run from starts[d+1] to starts[d+2].
func (b *IndexBuilder) planChildStartTail(starts []uint64) error {
	from, ok := b.corePlan.ChildStartTail()
	if !ok {
		return nil
	}
	var rec spoolRecord
	var vals []uint64
	for depth := len(b.spool.depths) - 1; depth >= 0 && starts[depth+2] >= from; depth-- {
		r, err := b.spool.open(depth)
		if err != nil {
			return err
		}
		for err == nil {
			if vals, err = r.next(&rec, vals[:0]); err == nil && starts[depth+1]+rec.childLocal >= from {
				b.corePlan.AddTail(rec.count, rec.bytes)
			}
		}
		if err := errors.Join(ignoreEOF(err), r.close()); err != nil {
			return err
		}
	}

	return nil
}

// ignoreEOF is err, or nil at the clean end of a stream.
func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

// openStatsWriters creates the depth index, core stats and tier stats
// writers. An index without rows has no tier plan and no tier files.
func (b *IndexBuilder) openStatsWriters() (*format.DepthIndexWriter, error) {
	depthW, err := format.NewDepthIndexWriter(b.outDir, b.checksums)
	if err != nil {
		return nil, fmt.Errorf("create depth index: %w", err)
	}
	enc := format.TierMaskPlain
	if b.tierPlan != nil {
		if b.tierW, err = format.NewTierStatsWriter(b.outDir, b.tierPlan, b.checksums); err != nil {
			return nil, errors.Join(fmt.Errorf("create tier stats writer: %w", err), depthW.Abort())
		}
		enc = b.tierPlan.MaskEncoding()
	}
	if b.coreW, err = format.NewCoreStatsWriter(b.outDir, &b.corePlan, enc, b.checksums); err != nil {
		return nil, errors.Join(fmt.Errorf("create core stats writer: %w", err), depthW.Abort())
	}

	return depthW, nil
}

// closeStatsWriters finishes the stats files. A writer whose close fails
// stays set for cleanup to abort.
func (b *IndexBuilder) closeStatsWriters(depthW *format.DepthIndexWriter) error {
	if err := b.coreW.Close(); err != nil {
		return errors.Join(fmt.Errorf("close core stats: %w", err), depthW.Abort())
	}
	b.coreW = nil
	if b.tierW != nil {
		if err := b.tierW.Close(); err != nil {
			return errors.Join(fmt.Errorf("close tier stats: %w", err), depthW.Abort())
		}
		b.tierW = nil
	}
	if err := depthW.Close(); err != nil {
		return fmt.Errorf("write depth index: %w", err)
	}

	return nil
}

// writeStatsRow writes one spooled row's stats, the next in level
// order: its tier stats, core row and depth-index entry.
func (b *IndexBuilder) writeStatsRow(depthW *format.DepthIndexWriter, starts []uint64, row int64, depth int, r *spoolRecord) error {
	if b.tierW != nil {
		if err := b.tierW.AddSlots(r.tierMask, r.tiers); err != nil {
			return fmt.Errorf("row %d at depth %d: write tier stats: %w", row, depth, err)
		}
	}
	if err := b.coreW.Add(r.count, r.bytes, starts[depth+1]+r.childLocal, r.tierMask); err != nil {
		return fmt.Errorf("write core stats row: %w", err)
	}
	if err := depthW.Add(uint32(depth), r.count, r.bytes); err != nil {
		return fmt.Errorf("add to depth index: %w", err)
	}

	return nil
}

// writeNameRow writes one spooled row's name, the next in level order:
// its prefix-tree row.
func (b *IndexBuilder) writeNameRow(starts []uint64, depth int, r *spoolRecord) error {
	var err error
	if depth == 0 {
		err = b.prefixTreeW.AddRoot()
	} else {
		err = b.prefixTreeW.Add(starts[depth-1]+r.parentLocal, r.segID)
	}
	if err != nil {
		return fmt.Errorf("prefix tree row at depth %d: %w", depth, err)
	}

	return nil
}

// MPHFFractionOfLimit is the share of GOMEMLIMIT the MPHF build's
// workers may hold.
const mphfFractionOfLimit = 0.25

// mphfMemoryBudget is the MPHF build's heap under memoryLimit, or 0,
// the builder's default, without a limit.
func mphfMemoryBudget(memoryLimit int64) int64 {
	if memoryLimit <= 0 || memoryLimit >= unsetMemoryLimit {
		return 0
	}

	return int64(float64(memoryLimit) * mphfFractionOfLimit)
}

// writeLevelsProcs is the cores writeLevels leaves the MPHF build that
// runs beside it at first; the build then grows to every core.
const writeLevelsProcs = 3

// finishMPHF reports the MPHF build that ran beside writeLevels as the
// next step, gives it every core and waits for it. The step's own log
// covers only the part after writeLevels, so the build's whole wall
// time and the part that overlapped writeLevels are logged here.
func (b *IndexBuilder) finishMPHF(ctx context.Context, h *format.MPHFBuild, started time.Time) error {
	besideWrites := time.Since(started)
	h.SetProgress(b.stepProgress)
	h.Grow(runtime.GOMAXPROCS(0))
	if err := h.Wait(); err != nil {
		return fmt.Errorf("build MPHF: %w", err)
	}
	total := h.Elapsed()
	overlap := min(besideWrites, total)
	zerolog.Ctx(ctx).Info().
		Str("duration", humanfmt.Duration(total)).
		Dur("duration_ms", total).
		Str("beside_row_writes", humanfmt.Duration(overlap)).
		Dur("beside_row_writes_ms", overlap).
		Msg("prefix hash build complete")

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
	data, err := tiers.EncodeManifest(b.resolvedTiers)
	if err != nil {
		return fmt.Errorf("write tier manifest: %w", err)
	}
	if err := format.WriteFile(filepath.Join(b.outDir, tiers.ManifestFile), data, b.checksums); err != nil {
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
