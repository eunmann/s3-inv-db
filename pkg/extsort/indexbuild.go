package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
// depth (levelSpool), whose writer goroutine also plans the core and
// tier files' widths from every row and interns its last segment;
// Finalize replays the depths in order into the index writers, which
// write each file in its final form.
//
// Memory is bounded: rows and prefix hashes go to disk, and the stack
// holds one entry per depth.
type IndexBuilder struct {
	// The plans and the interner are filled on the spool's writer
	// goroutine, and read once Finalize has stopped it.
	tierPlan *format.TierStatsPlan
	corePlan format.CoreStatsPlan
	interner *format.PrefixSegmentInterner
	// treePlan holds, per depth, the rows the spool's writer has seen
	// and the most a row's index within its depth exceeds its parent's:
	// with the depths' sizes that gives the widest parent distance.
	treePlan      treePlan
	coreW         *format.CoreStatsWriter
	tierW         *format.TierStatsWriter
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

// NewIndexBuilderWithCapacity creates a streaming index builder. The
// capacity hint is unused.
func NewIndexBuilderWithCapacity(outDir, tempDir string, _ uint64) (*IndexBuilder, error) {
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

	interner, err := format.NewPrefixSegmentInterner(outDir)
	if err != nil {
		mphfBuilder.Close()

		return nil, errors.Join(fmt.Errorf("create prefix segment interner: %w", err), os.RemoveAll(spoolDir))
	}

	b := &IndexBuilder{
		outDir:        outDir,
		tempDir:       tempDir,
		mphfBuilder:   mphfBuilder,
		interner:      interner,
		spool:         newLevelSpool(spoolDir),
		stack:         make([]stackEntry, 0, 32),
		createdOutDir: createdOutDir,
	}
	b.spool.plan = b.planRow

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

// planRow adds a row to the core, tier and tree plans and interns its
// last segment into r's segment ID. It runs on the spool's writer
// goroutine, which sees every row once, in Add order.
func (b *IndexBuilder) planRow(depth int, r *spoolRecord) error {
	mask, err := b.tierPlan.Add(&r.tierCounts, &r.tierBytes, r.count, r.bytes)
	if err != nil {
		return fmt.Errorf("prefix segment %q at depth %d: tier stats: %w", r.name, depth, err)
	}
	b.corePlan.Add(r.count, r.bytes, mask)
	if depth > 0 {
		// The interner copies a new segment, so the name may alias the
		// record's buffer.
		if r.segID, err = b.interner.Intern(unsafe.String(unsafe.SliceData(r.name), len(r.name))); err != nil {
			return fmt.Errorf("prefix segment %q at depth %d: %w", r.name, depth, err)
		}
	}
	b.treePlan.add(depth, r.parentLocal)

	return nil
}

// treePlan follows each depth's rows on the spool's writer goroutine.
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
	b.mphfBuilder.SetProgress(b.stepProgress)

	log.Debug().Msg("index builder: starting Finalize")

	b.closeNodesAbove(0)

	log.Debug().Uint64("prefix_count", b.posCount).Msg("index builder: writing rows in level order")

	if err := b.writeLevels(); err != nil {
		return err
	}
	if err := b.writeTierManifest(); err != nil {
		return err
	}

	log.Debug().
		Uint64("mphf_keys", b.mphfBuilder.Count()).
		Msg("index builder: building the MPHF")

	if err := b.buildMPHF(ctx); err != nil {
		return err
	}

	log.Debug().Msg("index builder: writing manifest")

	if err := format.WriteManifest(b.outDir, b.posCount, b.maxDepth, b.stepProgress); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	log.Debug().
		Str("total_duration", humanfmt.Duration(time.Since(startTime))).
		Msg("index builder: Finalize complete")

	return nil
}

// levelsProgressEvery is how many rows writeLevels writes between
// progress reports: a power of two, so the check is a mask.
const levelsProgressEvery = 1 << 17

// levelBatchRows and levelBatches size writeLevels' hand-off from the
// spool reader to the two writing sides: four batches of 512 records,
// about 0.6 MiB.
const (
	levelBatchRows = 512
	levelBatches   = 4
)

// levelBatch is a run of one depth's records. Both sides write it, and
// the one that finishes it last returns it to be refilled.
type levelBatch struct {
	recs    []spoolRecord
	depth   int
	pending atomic.Int32
}

// levelHandoff carries batches from the spool reader to the writing
// sides and back. Failed closes when either end fails: the reader then
// stops, and the sides drain what it already sent.
type levelHandoff struct {
	free     chan *levelBatch
	failed   chan struct{}
	sides    []chan *levelBatch
	failOnce sync.Once
}

func newLevelHandoff(sides int) *levelHandoff {
	h := &levelHandoff{free: make(chan *levelBatch, levelBatches), failed: make(chan struct{})}
	for range levelBatches {
		h.free <- &levelBatch{recs: make([]spoolRecord, levelBatchRows)}
	}
	for range sides {
		h.sides = append(h.sides, make(chan *levelBatch, levelBatches))
	}

	return h
}

func (h *levelHandoff) fail() { h.failOnce.Do(func() { close(h.failed) }) }

// take returns an empty batch to fill, or false once failed.
func (h *levelHandoff) take() (*levelBatch, bool) {
	select {
	case batch := <-h.free:
		batch.recs = batch.recs[:cap(batch.recs)]

		return batch, true
	case <-h.failed:
		return nil, false
	}
}

// send gives batch to every side, or returns false once failed.
func (h *levelHandoff) send(batch *levelBatch) bool {
	batch.pending.Store(int32(len(h.sides)))
	for _, side := range h.sides {
		select {
		case side <- batch:
		case <-h.failed:
			return false
		}
	}

	return true
}

// release is a side's end of batch; the last side to finish it frees it.
func (h *levelHandoff) release(batch *levelBatch) {
	if batch.pending.Add(-1) == 0 {
		h.free <- batch
	}
}

// drain writes every row of every batch on side i in order, until the
// first error, then only releases batches until the reader closes the
// side.
func (h *levelHandoff) drain(i int, write func(depth int, r *spoolRecord) error) error {
	var err error
	for batch := range h.sides[i] {
		for j := 0; err == nil && j < len(batch.recs); j++ {
			if err = write(batch.depth, &batch.recs[j]); err != nil {
				h.fail()
			}
		}
		h.release(batch)
	}

	return err
}

// writeLevels replays the spooled rows depth by depth into the index
// writers, so every per-row file is in level order. A row's child start
// and parent become rows once every depth's size is known: the first
// row of a depth is the number of rows above it. The writers take their
// widths from the plans the spool's writer filled.
func (b *IndexBuilder) writeLevels() error {
	if err := b.spool.finish(); err != nil {
		return err
	}
	// The dictionary is complete: closing the interner writes its end and
	// frees its segment map before the replay.
	err := b.interner.Close()
	segments := b.interner.Count()
	b.interner = nil
	if err != nil {
		return fmt.Errorf("write prefix dictionary: %w", err)
	}
	starts := make([]uint64, len(b.levelRows)+1)
	for d, n := range b.levelRows {
		starts[d+1] = starts[d] + n
	}
	total := int64(b.posCount)
	var written int64
	b.stepProgress(stepWriteLevels, 0, total)
	if err := b.planChildStartTail(starts); err != nil {
		return err
	}
	depthW, err := b.openStatsWriters()
	if err != nil {
		return err
	}
	if b.prefixTreeW, err = format.NewPrefixTreeWriter(b.outDir, b.posCount, segments, b.treePlan.maxDist(b.levelRows)); err != nil {
		return errors.Join(fmt.Errorf("create prefix tree: %w", err), depthW.Abort())
	}
	err = b.replayLevels(func(depth int, r *spoolRecord) error {
		written++
		if written%levelsProgressEvery == 0 {
			b.stepProgress(stepWriteLevels, written, total)
		}

		return b.writeStatsRow(depthW, starts, depth, r)
	}, func(depth int, r *spoolRecord) error { return b.writeNameRow(starts, depth, r) })
	if err != nil {
		return errors.Join(err, depthW.Abort())
	}
	if err := b.closeStatsWriters(depthW); err != nil {
		return err
	}
	if err := b.prefixTreeW.Close(); err != nil {
		return fmt.Errorf("close prefix tree: %w", err)
	}
	b.prefixTreeW = nil
	b.stepProgress(stepWriteLevels, total, total)

	return os.RemoveAll(b.spool.dir) //nolint:wrapcheck // the path is in the error
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
	for depth := len(b.spool.depths) - 1; depth >= 0 && starts[depth+2] >= from; depth-- {
		r, err := b.spool.open(depth)
		if err != nil {
			return err
		}
		for err == nil {
			if err = r.next(&rec); err == nil && starts[depth+1]+rec.childLocal >= from {
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
	depthW, err := format.NewDepthIndexWriter(b.outDir)
	if err != nil {
		return nil, fmt.Errorf("create depth index: %w", err)
	}
	enc := format.TierMaskPlain
	if b.tierPlan != nil {
		if b.tierW, err = format.NewTierStatsWriter(b.outDir, b.tierPlan); err != nil {
			return nil, errors.Join(fmt.Errorf("create tier stats writer: %w", err), depthW.Abort())
		}
		enc = b.tierPlan.MaskEncoding()
	}
	if b.coreW, err = format.NewCoreStatsWriter(b.outDir, &b.corePlan, enc); err != nil {
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

// replayLevels reads the spool once into batches for two sides that
// share nothing and write side by side: the stats rows (tier stats,
// core stats, depth index) on this goroutine, and the names (prefix
// tree, MPHF keys) on their own. Each side gets every row in level
// order. Reading each depth twice, once per side, was half of the stats
// side's time. A side's error stops the reader and the other side.
func (b *IndexBuilder) replayLevels(stats, names func(depth int, r *spoolRecord) error) error {
	const statsSide, namesSide = 0, 1
	h := newLevelHandoff(2)
	var readErr, namesErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		if readErr = b.readLevels(h); readErr != nil {
			h.fail()
		}
	})
	wg.Go(func() { namesErr = h.drain(namesSide, names) })
	statsErr := h.drain(statsSide, stats)
	wg.Wait()

	return errors.Join(readErr, statsErr, namesErr)
}

// readLevels reads every depth's records once, in order, and hands
// them to the sides; each depth's file is removed once read. It closes
// the sides when done, or once the handoff failed (returning nil: the
// failure is the side's to report).
func (b *IndexBuilder) readLevels(h *levelHandoff) error {
	defer func() {
		for _, side := range h.sides {
			close(side)
		}
	}()
	for depth := range len(b.spool.depths) {
		r, err := b.spool.open(depth)
		if err != nil {
			return err
		}
		done, err := readDepth(r, h)
		if err = errors.Join(err, r.close()); err != nil || !done {
			return err
		}
		if err := b.spool.remove(depth); err != nil {
			return err
		}
	}

	return nil
}

// readDepth hands r's records to the sides in batches: done is false
// when the handoff failed first.
func readDepth(r *spoolReader, h *levelHandoff) (bool, error) {
	for {
		batch, ok := h.take()
		if !ok {
			return false, nil
		}
		n := 0
		var err error
		for ; n < len(batch.recs); n++ {
			if err = r.next(&batch.recs[n]); err != nil {
				break
			}
		}
		if n == 0 {
			h.free <- batch
		} else {
			batch.recs, batch.depth = batch.recs[:n], r.depth
			if !h.send(batch) {
				return false, nil
			}
		}
		if errors.Is(err, io.EOF) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
	}
}

// writeStatsRow writes one spooled row's stats, the next in level
// order: its tier stats, core row and depth-index entry.
func (b *IndexBuilder) writeStatsRow(depthW *format.DepthIndexWriter, starts []uint64, depth int, r *spoolRecord) error {
	var mask uint16
	if b.tierW != nil {
		var err error
		if mask, err = b.tierW.Add(&r.tierCounts, &r.tierBytes); err != nil {
			return fmt.Errorf("prefix segment %q at depth %d: write tier stats: %w", r.name, depth, err)
		}
	}
	if err := b.coreW.Add(r.count, r.bytes, starts[depth+1]+r.childLocal, mask); err != nil {
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
		err = b.prefixTreeW.Add(starts[depth-1]+r.parentLocal, r.segID)
	}
	if err != nil {
		return fmt.Errorf("prefix tree row at depth %d: %w", depth, err)
	}
	if err := b.mphfBuilder.AddHashed(r.keyHash, r.fp); err != nil {
		return fmt.Errorf("add to MPHF builder: %w", err)
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
