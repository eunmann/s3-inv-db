package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// The parallel replay. Every per-row output has its place before the
// replay starts: a row's level-order position is its depth's start plus
// its index within the depth, and the spool's marks give the multi tier
// layout's positions from each mark on. So the rows from one mark to the
// next (a chunk) can be decoded and written on their own: replay workers
// take the chunks in level order, each decoding its chunk from the
// depth's file and writing its rows straight to their places through the
// format package's range writers. What a chunk's place does not fix
// (core patch exceptions, the bit vectors' words and rank directories,
// the checks the sequential writers make as they close) comes back in a
// chunkResult, which the calling goroutine merges in chunk order.
//
// Memory is per worker: a spool read buffer (levelSpoolBufSize) and a
// range buffer per file it writes (format's rangeBufSize, six files at
// most), about 1 MiB; per chunk in flight, of which there are at most
// replayWindow per worker, the chunk's bit vector words (two bits per
// row) and core exceptions (24 B each, at most the plan's); and the
// chunk list, 56 B per spoolMarkRows rows.

// replayWindow is the chunks per worker that may be in flight ahead of
// the merge: a worker never waits on a slow chunk before it unless it is
// that many chunks ahead.
const replayWindow = 2

// errSpoolMarks means a depth's spool file holds more rows than its
// marks count.
var errSpoolMarks = errors.New("level spool rows differ from its marks")

// levelChunk is the rows of one depth from one spool mark to the next.
type levelChunk struct {
	depth int
	// local is the first row's index within its depth.
	local, rows uint64
	// off and end are the rows' bytes in the depth's spool file.
	off, end uint64
	// multi and stored are the multi-tier rows and stored tier slots
	// before the chunk, in level order.
	multi, stored uint64
}

// chunks cuts every depth's spooled rows at its marks, in level order.
func (s *levelSpool) chunks() []levelChunk {
	var out []levelChunk
	var multi, stored uint64
	for d, sf := range s.depths {
		for j, m := range sf.marks {
			c := levelChunk{
				depth: d, local: uint64(j) * s.markEvery, off: m.off, end: sf.size,
				multi: multi + m.multi, stored: stored + m.stored,
			}
			c.rows = min(s.markEvery, sf.rows-c.local)
			if j+1 < len(sf.marks) {
				c.end = sf.marks[j+1].off
			}
			out = append(out, c)
		}
		multi += sf.multi
		stored += sf.stored
	}

	return out
}

// levelRanges are the index files the replay writes, as range writers.
type levelRanges struct {
	core  *format.CoreStatsRanges
	tier  *format.TierStatsRanges // nil for an index without rows
	tree  *format.PrefixTreeRanges
	depth *format.DepthIndexRanges
}

// openLevelRanges creates the files the replay writes at their final
// sizes.
func (b *IndexBuilder) openLevelRanges(segments uint32) (*levelRanges, error) {
	l := &levelRanges{}
	var err error
	if l.depth, err = format.NewDepthIndexRanges(b.outDir, b.levelRows, b.checksums); err != nil {
		return nil, fmt.Errorf("create depth index: %w", err)
	}
	enc := format.TierMaskPlain
	if b.tierPlan != nil {
		if l.tier, err = format.NewTierStatsRanges(b.outDir, b.tierPlan, b.checksums); err != nil {
			return nil, errors.Join(fmt.Errorf("create tier stats writer: %w", err), l.abort())
		}
		enc = b.tierPlan.MaskEncoding()
	}
	if l.core, err = format.NewCoreStatsRanges(b.outDir, &b.corePlan, enc, b.checksums); err != nil {
		return nil, errors.Join(fmt.Errorf("create core stats writer: %w", err), l.abort())
	}
	if l.tree, err = format.NewPrefixTreeRanges(b.outDir, b.posCount, segments, b.treePlan.maxDist(b.levelRows), b.checksums); err != nil {
		return nil, errors.Join(fmt.Errorf("create prefix tree: %w", err), l.abort())
	}

	return l, nil
}

// merge takes a chunk's parts, the next in level order.
func (l *levelRanges) merge(res *chunkResult) error {
	if err := l.core.Merge(&res.core); err != nil {
		return fmt.Errorf("merge core stats: %w", err)
	}
	if l.tier != nil {
		if err := l.tier.Merge(&res.tier); err != nil {
			return fmt.Errorf("merge tier stats: %w", err)
		}
	}
	if err := l.tree.Merge(&res.tree); err != nil {
		return fmt.Errorf("merge prefix tree: %w", err)
	}
	if err := l.depth.Merge(&res.depth); err != nil {
		return fmt.Errorf("merge depth index: %w", err)
	}

	return nil
}

// close finishes every file once every chunk is merged; on an error it
// removes them all.
func (l *levelRanges) close() error {
	if err := l.core.Close(); err != nil {
		return errors.Join(fmt.Errorf("close core stats: %w", err), l.abort())
	}
	if l.tier != nil {
		if err := l.tier.Close(); err != nil {
			return errors.Join(fmt.Errorf("close tier stats: %w", err), l.abort())
		}
	}
	if err := l.tree.Close(); err != nil {
		return errors.Join(fmt.Errorf("close prefix tree: %w", err), l.abort())
	}
	if err := l.depth.Close(); err != nil {
		return errors.Join(fmt.Errorf("write depth index: %w", err), l.abort())
	}

	return nil
}

// abort removes every file the writers started and did not finish.
func (l *levelRanges) abort() error {
	var errs []error
	if l.core != nil {
		errs = append(errs, l.core.Abort())
	}
	if l.tier != nil {
		errs = append(errs, l.tier.Abort())
	}
	if l.tree != nil {
		errs = append(errs, l.tree.Abort())
	}
	if l.depth != nil {
		errs = append(errs, l.depth.Abort())
	}

	return errors.Join(errs...)
}

// chunkResult is what a worker leaves of a chunk for the merge.
type chunkResult struct {
	core  format.CoreStatsPart
	tier  format.TierStatsPart
	tree  format.PrefixTreePart
	depth format.DepthIndexPart
	err   error
	busy  time.Duration
}

// replayWorker decodes and writes chunks, one goroutine's.
type replayWorker struct {
	in    *spoolReader
	core  *format.CoreStatsRange
	tier  *format.TierStatsRange
	tree  *format.PrefixTreeRange
	depth *format.DepthIndexRange
	vals  []uint64
	rec   spoolRecord
}

func (l *levelRanges) newWorker() *replayWorker {
	w := &replayWorker{
		in:    newSectionReader(),
		core:  l.core.NewRange(),
		tree:  l.tree.NewRange(),
		depth: l.depth.NewRange(),
	}
	if l.tier != nil {
		w.tier = l.tier.NewRange()
	}

	return w
}

// chunk decodes c from f, its depth's spool file, and writes its rows;
// starts are the depths' first rows.
func (w *replayWorker) chunk(f *os.File, starts []uint64, c *levelChunk, res *chunkResult) error {
	w.in.section(f, c.depth, c.off, c.end)
	row := starts[c.depth] + c.local
	err := errors.Join(w.core.Start(row, &res.core), w.tree.Start(row, &res.tree), w.depth.Start(c.depth, c.local, &res.depth))
	if w.tier != nil {
		err = errors.Join(err, w.tier.Start(row, c.multi, c.stored, &res.tier))
	}
	if err != nil {
		return fmt.Errorf("start rows %d to %d at depth %d: %w", row, row+c.rows, c.depth, err)
	}
	for i := range c.rows {
		w.vals, err = w.in.next(&w.rec, w.vals[:0])
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("level spool %d: %w: %d rows from row %d, %d marked", c.depth, errSpoolTruncated, i, c.local, c.rows)
		}
		if err != nil {
			return err
		}
		if err := w.write(starts, row+i, c.depth, &w.rec); err != nil {
			return err
		}
	}
	if _, err := w.in.next(&w.rec, w.vals[:0]); !errors.Is(err, io.EOF) {
		return errors.Join(fmt.Errorf("level spool %d: %w: more than the %d rows from row %d", c.depth, errSpoolMarks, c.rows, c.local), err)
	}
	err = errors.Join(w.core.Flush(), w.tree.Flush(), w.depth.Flush())
	if w.tier != nil {
		err = errors.Join(err, w.tier.Flush())
	}

	return err
}

// write writes one spooled row, row in level order: its tier stats,
// core row, depth-index entry and prefix-tree row.
func (w *replayWorker) write(starts []uint64, row uint64, depth int, r *spoolRecord) error {
	if w.tier != nil {
		if err := w.tier.AddSlots(r.tierMask, r.tiers); err != nil {
			return fmt.Errorf("row %d at depth %d: write tier stats: %w", row, depth, err)
		}
	}
	if err := w.core.Add(r.count, r.bytes, starts[depth+1]+r.childLocal, r.tierMask); err != nil {
		return fmt.Errorf("write core stats row: %w", err)
	}
	if err := w.depth.Add(r.count, r.bytes); err != nil {
		return fmt.Errorf("add to depth index: %w", err)
	}
	var err error
	if depth == 0 {
		err = w.tree.AddRoot()
	} else {
		err = w.tree.Add(starts[depth-1]+r.parentLocal, r.segID)
	}
	if err != nil {
		return fmt.Errorf("prefix tree row at depth %d: %w", depth, err)
	}

	return nil
}

// chunkReplay is one parallel replay: the chunks, the spool files the
// workers read them from, and the hand-off from the workers to the
// merge. Results and done have a slot per chunk in flight, chunk i
// using slot i%len(results); tokens holds one per free slot, so a
// worker takes chunk i only once chunk i-len(results) is merged.
type chunkReplay struct {
	levels  *levelRanges
	starts  []uint64
	chunks  []levelChunk
	files   []*os.File // per depth; nil once the depth's chunks are merged
	results []chunkResult
	done    []chan struct{}
	tokens  chan struct{}
	stop    chan struct{}
	next    atomic.Int64
}

// replayChunks has workers goroutines decode and write chunks while this
// goroutine merges their results in level order, removing each depth's
// spool file once its last chunk is merged. Any chunk's error, or ctx's
// end, stops every worker before its next chunk; replayChunks returns
// once they all have. The caller finishes or aborts levels.
func (b *IndexBuilder) replayChunks(ctx context.Context, levels *levelRanges, starts []uint64, chunks []levelChunk, workers int) (replayBusy, error) {
	window := replayWindow * workers
	r := &chunkReplay{
		levels: levels, starts: starts, chunks: chunks,
		files:   make([]*os.File, len(b.spool.depths)),
		results: make([]chunkResult, window),
		done:    make([]chan struct{}, window),
		tokens:  make(chan struct{}, window),
		stop:    make(chan struct{}),
	}
	defer r.closeFiles()
	for d, sf := range b.spool.depths {
		f, err := os.Open(sf.path)
		if err != nil {
			return replayBusy{}, fmt.Errorf("open level spool %d: %w", d, err)
		}
		r.files[d] = f
	}
	for i := range window {
		r.done[i] = make(chan struct{}, 1)
		r.tokens <- struct{}{}
	}
	busy := replayBusy{workers: workers}
	start := time.Now()
	var wg sync.WaitGroup
	for range workers {
		w := levels.newWorker()
		wg.Go(func() { r.work(ctx, w) })
	}
	err := b.mergeChunks(r, &busy)
	close(r.stop)
	wg.Wait()
	busy.wall = time.Since(start)

	return busy, err
}

// work decodes and writes chunks, taking the next once it holds a token,
// until there are none left or the replay stops.
func (r *chunkReplay) work(ctx context.Context, w *replayWorker) {
	for {
		select {
		case <-r.tokens:
		case <-r.stop:
			return
		}
		select {
		case <-r.stop:
			return
		default:
		}
		i := int(r.next.Add(1) - 1)
		if i >= len(r.chunks) {
			return
		}
		c, slot := &r.chunks[i], i%len(r.results)
		res := &r.results[slot]
		began := time.Now()
		if err := ctx.Err(); err != nil {
			res.err = fmt.Errorf("index build cancelled: %w", err)
		} else {
			res.err = w.chunk(r.files[c.depth], r.starts, c, res)
		}
		res.busy = time.Since(began)
		r.done[slot] <- struct{}{}
	}
}

// mergeChunks merges the chunks' results in level order as the workers
// finish them, handing a token back for each, until the first error.
func (b *IndexBuilder) mergeChunks(r *chunkReplay, busy *replayBusy) error {
	total := int64(b.posCount)
	var written int64
	for i := range r.chunks {
		slot := i % len(r.results)
		<-r.done[slot]
		began := time.Now()
		res, c := &r.results[slot], &r.chunks[i]
		if res.err != nil {
			return res.err
		}
		if err := r.levels.merge(res); err != nil {
			return err
		}
		busy.work += res.busy
		if i+1 == len(r.chunks) || r.chunks[i+1].depth != c.depth {
			err := r.files[c.depth].Close()
			r.files[c.depth] = nil
			if err != nil {
				return fmt.Errorf("close level spool %d: %w", c.depth, err)
			}
			if err := b.spool.remove(c.depth); err != nil {
				return err
			}
		}
		written += int64(c.rows)
		b.stepProgress(stepWriteLevels, written, total)
		busy.merge += time.Since(began)
		r.tokens <- struct{}{}
	}

	return nil
}

func (r *chunkReplay) closeFiles() {
	for _, f := range r.files {
		if f != nil {
			_ = f.Close()
		}
	}
}
