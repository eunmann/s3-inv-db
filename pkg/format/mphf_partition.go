package format

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// The partitioned build. A key's partition is its hash's top
// partition bits, so a partition's keys sit in one spill file, or in a
// run of them when partitions are coarser than spill files; when they
// are finer, each spill file is first split into its partitions' own
// files. Every partition's key count is known from the Add side's
// counts before any is read, so its slots, [base, base+count), are
// known too: a worker reads a partition's records, builds its table,
// lays out its slots and writes them with one pwrite, and hands the
// table to the writer, which appends the tables to mph.bin in order.
//
// A worker holds one partition's records and buffers (partScratch,
// about partKeyBytes plus the slot width per key), and the writer at
// most two tables per worker that wait on an earlier one, so the build's
// heap is bounded by the largest partition times the workers, whom the
// memory budget caps.
const (
	// TargetPartitionBits is log2 of the keys a partition aims at while
	// there are at most spillBuckets partitions: a partition's records
	// and levels then stay in a core's L2 as they are built, and a small
	// build's workers each hold well under a MiB.
	targetPartitionBits = 13
	// MaxPartitionKeys is the most keys a partition holds when the
	// partition bits allow it: past spillBuckets of them, partitions
	// grow finer instead of larger, so a worker's heap stays bounded.
	maxPartitionKeys = 1 << 20
	// PartKeyBytes is a worker's heap per key of its largest partition
	// besides the slot layout: the hashes, fingerprints and positions
	// read, the colliding keys, level marks and rank directory, and its
	// share of the tables waiting for the writer.
	partKeyBytes = 34
	// DefaultMPHFBudget is the workers' heap when SetMemoryBudget was
	// not called.
	defaultMPHFBudget = 256 << 20
	// MaxSplitters bounds the goroutines splitting spill files into
	// partition files, each with a write buffer per partition of its
	// bucket.
	maxSplitters = 4
	// SpillReadBytes is a worker's read buffer: whole records.
	spillReadBytes = spillRecordBytes * 683
)

// ErrMPHFAborted is the error of a build that Abort ended.
var ErrMPHFAborted = errors.New("MPHF build aborted")

// mphfPlan is a build's partitions, from the Add side's key counts.
type mphfPlan struct {
	n       uint64
	bits    uint     // partition bits
	counts  []uint64 // keys per partition
	bases   []uint64 // first slot per partition
	buckets []uint64 // keys per spill file
	maxPart uint64
	posW    int
	slotW   int
}

// planPartitions sizes the partitions of n keys whose counts by their
// hashes' top maxPartBits bits are hist (see choosePartBits); forceBits,
// when not negative, fixes the partition bits.
func planPartitions(hist []uint64, n uint64, forceBits int, maxKeys uint64) mphfPlan {
	p := mphfPlan{n: n, posW: int(byteWidthOf(max(n, 1) - 1))}
	p.slotW = mphfFingerprintBytes + p.posW
	if n == 0 {
		return p
	}
	p.bits = choosePartBits(hist, n, maxKeys)
	if forceBits >= 0 {
		p.bits = uint(min(forceBits, maxPartBits))
	}
	p.counts = partitionCounts(hist, p.bits)
	p.bases = make([]uint64, len(p.counts))
	var base uint64
	for i, c := range p.counts {
		p.bases[i] = base
		base += c
		p.maxPart = max(p.maxPart, c)
	}
	p.buckets = partitionCounts(hist, spillBits)

	return p
}

// choosePartBits picks the partition bits for n keys: enough for about
// 2^targetPartitionBits keys a partition, up to spillBuckets
// partitions, then more while the largest partition holds more than
// maxKeys keys. They depend only on the keys, never on the order they
// were added in or the workers, so the build's output does not either.
func choosePartBits(hist []uint64, n, maxKeys uint64) uint {
	k := min(uint(max(0, bits.Len64(n-1)-targetPartitionBits)), spillBits)
	for k < maxPartBits && slices.Max(partitionCounts(hist, k)) > maxKeys {
		k++
	}

	return k
}

// partitionCounts sums hist's counts into 2^k partitions.
func partitionCounts(hist []uint64, k uint) []uint64 {
	counts := make([]uint64, 1<<k)
	for i, c := range hist {
		counts[i>>(maxPartBits-k)] += c
	}

	return counts
}

// MPHFBuild is a build Start began: workers build the partitions and
// the writer appends them to mph.bin in order. Wait ends it.
type MPHFBuild struct {
	b          *StreamingMPHFBuilder
	sums       *Checksums
	outDir     string
	starts     []uint64
	plan       mphfPlan
	maxWorkers int
	stopCtx    func() bool

	mu   sync.Mutex
	cond sync.Cond
	// err is the build's first failure; stopped is set with it, and
	// every goroutine stops at its next check.
	err     error
	stopped bool
	// The workers wanted (Grow raises it) and started. Workers start
	// once the build is past preparing, and while the writer runs.
	target   int
	running  int
	building bool
	writing  bool
	waited   bool
	// next is the next partition to claim and written the partitions
	// in mph.bin. Partition p builds into ring[p%len(ring)], claimed
	// only once the partition len(ring) before it is written.
	next    int
	written int
	ring    []bbPartition
	ready   []bool
	stopAll atomic.Bool

	wg       sync.WaitGroup
	waitOnce sync.Once
	waitErr  error

	progress atomic.Pointer[stepCounter]
	done     atomic.Int64
	// posSum and wantSum sum a mix of every slot's position and of
	// every position in [0, n): equal when positions are one per key.
	posSum  atomic.Uint64
	wantSum atomic.Uint64

	mphFile, slotsFile *hashedFile
	created            []string

	// started is when Start began the build and finished when its
	// writer stopped.
	started, finished time.Time
}

func startMPHFBuild(ctx context.Context, b *StreamingMPHFBuilder, outDir string, starts []uint64, workers int, sums *Checksums) *MPHFBuild {
	plan := planPartitions(b.spill.hist, b.count, b.partBits, b.maxPartKeys)
	perWorker := int64(plan.maxPart)*int64(partKeyBytes+plan.slotW) + spillReadBytes
	maxWorkers := max(1, min(runtime.GOMAXPROCS(0), len(plan.counts), int(b.budget/perWorker)))
	h := &MPHFBuild{
		b:          b,
		sums:       sums,
		outDir:     outDir,
		starts:     starts,
		plan:       plan,
		maxWorkers: maxWorkers,
		target:     min(max(1, workers), maxWorkers),
		writing:    true,
		ring:       make([]bbPartition, 2*maxWorkers),
		ready:      make([]bool, 2*maxWorkers),
		started:    time.Now(),
	}
	h.cond.L = &h.mu
	h.stopCtx = context.AfterFunc(ctx, func() {
		h.fail(fmt.Errorf("build MPHF: %w", context.Cause(ctx)))
	})
	h.wg.Add(1)
	go h.run()

	return h
}

// Grow lets the build use n more workers, up to its memory budget.
func (h *MPHFBuild) Grow(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.target = min(h.maxWorkers, h.target+n)
	if h.building && h.writing && !h.stopped {
		h.startWorkersLocked()
	}
}

// SetProgress makes the build report StepBuildHash to fn from now on,
// counting the keys built so far.
func (h *MPHFBuild) SetProgress(fn StepProgress) {
	c := newStepCounter(fn, StepBuildHash, int64(h.plan.n))
	if c != nil {
		c.add(int(h.done.Load()))
	}
	h.progress.Store(c)
}

// Abort stops the build: Wait then removes its output and returns
// ErrMPHFAborted. A build already waited for is left as it is.
func (h *MPHFBuild) Abort() {
	h.mu.Lock()
	waited := h.waited
	h.mu.Unlock()
	if !waited {
		h.fail(ErrMPHFAborted)
	}
}

// Wait waits for the build to end and returns its error. On success
// mph.bin and mph_slots.bin are complete; on failure the files this
// build created are removed. Either way the spill files are.
func (h *MPHFBuild) Wait() error {
	h.waitOnce.Do(func() {
		h.wg.Wait()
		h.stopCtx()
		h.mu.Lock()
		h.waited = true
		err := h.err
		h.mu.Unlock()
		if err == nil {
			err = h.complete()
		}
		if err != nil {
			err = errors.Join(err, h.discard())
		}
		h.waitErr = errors.Join(err, h.b.spill.close())
		if c := h.progress.Load(); h.waitErr == nil && c != nil {
			c.finish()
		}
	})

	return h.waitErr
}

// Elapsed is the build's own wall time, from Start until its last
// partition was written or it stopped. It is valid once Wait returned.
func (h *MPHFBuild) Elapsed() time.Duration {
	return h.finished.Sub(h.started)
}

// fail records the build's first error and stops it.
func (h *MPHFBuild) fail(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err == nil {
		h.err = err
	}
	h.stopped = true
	h.stopAll.Store(true)
	h.cond.Broadcast()
}

func (h *MPHFBuild) startWorkersLocked() {
	for ; h.running < h.target; h.running++ {
		h.wg.Add(1)
		go h.worker()
	}
}

// run prepares the output files and partitions, starts the workers and
// writes mph.bin.
func (h *MPHFBuild) run() {
	defer h.wg.Done()
	defer func() { h.finished = time.Now() }()
	err := h.prepare()
	h.mu.Lock()
	if err != nil || h.stopped || h.plan.n == 0 {
		h.writing = false
		h.mu.Unlock()
		if err != nil {
			h.fail(err)
		}

		return
	}
	h.building = true
	h.startWorkersLocked()
	h.mu.Unlock()
	err = h.writeParts()
	h.mu.Lock()
	h.writing = false
	h.mu.Unlock()
	if err != nil {
		h.fail(err)
	}
}

// prepare creates the output files and, when partitions are finer than
// spill files, splits the spill files into partition files.
func (h *MPHFBuild) prepare() error {
	if err := h.createOutputs(); err != nil {
		return err
	}
	if h.plan.n > 0 && h.plan.bits > spillBits {
		return h.split()
	}

	return nil
}

// createOutputs creates mph.bin, empty, and mph_slots.bin at its full
// size with its header.
func (h *MPHFBuild) createOutputs() error {
	mphPath := filepath.Join(h.outDir, MPHFile)
	f, err := createHashedFile(mphPath, h.sums)
	if err != nil {
		return fmt.Errorf("create MPHF file: %w", err)
	}
	h.mphFile, h.created = f, append(h.created, mphPath)
	slotsPath := filepath.Join(h.outDir, MPHFSlotsFile)
	if f, err = createHashedFile(slotsPath, h.sums); err != nil {
		return fmt.Errorf("create MPHF slots: %w", err)
	}
	h.slotsFile, h.created = f, append(h.created, slotsPath)
	n, slotW := h.plan.n, uint64(h.plan.slotW)
	if err := f.truncate(int64(mphfSlotsDataOff + n*slotW + mphfSlotsTailPad)); err != nil {
		return fmt.Errorf("truncate MPHF slots: %w", err)
	}
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: n, Width: uint32(slotW)})
	if _, err := f.WriteAt(append(head, byte(h.plan.posW)), 0); err != nil {
		return fmt.Errorf("write MPHF slots header: %w", err)
	}

	return nil
}

// complete checks a build that ran to the end and closes its files.
func (h *MPHFBuild) complete() error {
	if h.written != len(h.plan.counts) {
		return fmt.Errorf("%w: %d of %d partitions written", ErrMPHFLookupFailed, h.written, len(h.plan.counts))
	}
	if h.posSum.Load() != h.wantSum.Load() {
		return fmt.Errorf("%w: the %d positions are not one per key", ErrMPHFPosition, h.plan.n)
	}
	for _, f := range []*hashedFile{h.mphFile, h.slotsFile} {
		f.startWriteback()
	}
	err := h.mphFile.Close()
	h.mphFile = nil
	if err != nil {
		return fmt.Errorf("close MPHF file: %w", err)
	}
	err = h.slotsFile.Close()
	h.slotsFile = nil
	if err != nil {
		return fmt.Errorf("close MPHF slots: %w", err)
	}

	return nil
}

// discard closes and removes the output files this build created.
func (h *MPHFBuild) discard() error {
	var errs []error
	for _, f := range []*hashedFile{h.mphFile, h.slotsFile} {
		if f != nil {
			errs = append(errs, f.Close())
		}
	}
	h.mphFile, h.slotsFile = nil, nil
	for _, path := range h.created {
		errs = append(errs, removeIfErr(path))
	}

	return errors.Join(errs...)
}

// writeParts appends every partition's table to mph.bin in order, as
// the workers finish them, after the partition bits.
func (h *MPHFBuild) writeParts() error {
	w := bufio.NewWriterSize(h.mphFile, bbhashIOBuffer)
	if err := putU64(w, uint64(h.plan.bits)); err != nil {
		return fmt.Errorf("write MPHF file: %w", err)
	}
	for p := range h.plan.counts {
		slot := p % len(h.ring)
		h.mu.Lock()
		for !h.stopped && !h.ready[slot] {
			h.cond.Wait()
		}
		stopped := h.stopped
		h.mu.Unlock()
		if stopped {
			return nil
		}
		if err := writePartition(w, &h.ring[slot]); err != nil {
			return fmt.Errorf("write MPHF file: %w", err)
		}
		h.mu.Lock()
		h.ready[slot] = false
		h.written = p + 1
		h.cond.Broadcast()
		h.mu.Unlock()
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("write MPHF file: %w", err)
	}

	return nil
}

// worker builds partitions until none is left or the build stops.
func (h *MPHFBuild) worker() {
	defer h.wg.Done()
	var s partScratch
	read := make([]byte, spillReadBytes)
	for {
		p, out, ok := h.claim()
		if !ok {
			return
		}
		if err := h.buildPart(p, out, &s, read); err != nil {
			h.fail(fmt.Errorf("MPHF partition %d: %w", p, err))

			return
		}
		h.mu.Lock()
		h.ready[p%len(h.ring)] = true
		h.cond.Broadcast()
		h.mu.Unlock()
	}
}

// claim returns the next partition and the table to build it into,
// once that table is free, or false when none is left or the build
// stopped.
func (h *MPHFBuild) claim() (int, *bbPartition, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for !h.stopped && h.next < len(h.plan.counts) && h.next >= h.written+len(h.ring) {
		h.cond.Wait()
	}
	if h.stopped || h.next >= len(h.plan.counts) {
		return 0, nil, false
	}
	p := h.next
	h.next++

	return p, &h.ring[p%len(h.ring)], true
}

// buildPart reads partition p's records, builds its table into out and
// writes its slots.
func (h *MPHFBuild) buildPart(p int, out *bbPartition, s *partScratch, read []byte) error {
	n := h.plan.counts[p]
	if n == 0 {
		*out = bbPartition{levels: out.levels[:0], bits: out.bits[:0], fallback: out.fallback[:0]}

		return nil
	}
	if err := h.readPart(p, s, read); err != nil {
		return err
	}
	if err := buildPartition(s.hashes, s.fps, s, out); err != nil {
		return err
	}
	if err := h.writeSlots(p, out, s); err != nil {
		return err
	}
	h.done.Add(int64(n))
	if c := h.progress.Load(); c != nil {
		c.add(int(n))
	}

	return nil
}

// readPart reads partition p's records into s, resolving each position
// key, and removes the files read.
func (h *MPHFBuild) readPart(p int, s *partScratch, read []byte) error {
	n := h.plan.counts[p]
	if uint64(cap(s.hashes)) < n {
		// Room for the next partitions too, about as large as this one,
		// within the largest the worker count is budgeted on.
		size := min(n+n/16, h.plan.maxPart)
		s.hashes, s.fps, s.pos = make([]uint64, 0, size), make([]uint64, 0, size), make([]uint64, 0, size)
		s.work, s.marks = make([]uint64, size), make([]uint64, 2*levelWords(int(size)))
	}
	s.hashes, s.fps, s.pos = s.hashes[:0], s.fps[:0], s.pos[:0]
	decode := func(recs []byte) error {
		for r := 0; r < len(recs); r += spillRecordBytes {
			pos, err := h.position(binary.LittleEndian.Uint64(recs[r+16:]))
			if err != nil {
				return err
			}
			s.hashes = append(s.hashes, binary.LittleEndian.Uint64(recs[r:]))
			s.fps = append(s.fps, binary.LittleEndian.Uint64(recs[r+8:]))
			s.pos = append(s.pos, pos)
		}

		return nil
	}
	if h.plan.bits > spillBits {
		return readSpill(&spillFile{path: partPath(h.b.spill.dir, p)}, n, read, decode)
	}
	per := 1 << (spillBits - h.plan.bits)
	for b := p * per; b < (p+1)*per; b++ {
		if h.plan.buckets[b] > 0 {
			if err := readSpill(&h.b.spill.files[b], h.plan.buckets[b], read, decode); err != nil {
				return err
			}
		}
	}

	return nil
}

// position resolves a position key against the depths' first
// positions: within its depth's, and so under the key count.
func (h *MPHFBuild) position(posKey uint64) (uint64, error) {
	if h.starts == nil {
		if posKey >= h.plan.n {
			return 0, fmt.Errorf("%w: position %d of %d keys", ErrMPHFPosition, posKey, h.plan.n)
		}

		return posKey, nil
	}
	depth, local := posKey>>posLocalBits, posKey&posLocalMask
	if depth+1 >= uint64(len(h.starts)) || local >= h.starts[depth+1]-h.starts[depth] {
		return 0, fmt.Errorf("%w: index %d at depth %d is past the depth", ErrMPHFPosition, local, depth)
	}

	return h.starts[depth] + local, nil
}

// writeSlots lays out partition p's slots from its records and table
// and writes them to its range of the slot file. Each slot takes two
// 8-byte stores, its fingerprint and its position. The position's store
// also covers the first 8-posW bytes of the next slot, which may be
// laid out already, so it merges into them: those bytes keep their
// value, laid out or not, and the next slot's fingerprint store writes
// them whole. Every slot is laid out once: the table maps the
// partition's keys one to one onto them.
func (h *MPHFBuild) writeSlots(p int, out *bbPartition, s *partScratch) error {
	n, slotW := len(s.hashes), uint64(h.plan.slotW)
	size := uint64(n)*slotW + mphfSlotsTailPad
	if uint64(cap(s.layout)) < size {
		s.layout = make([]byte, min(size+size/16, h.plan.maxPart*slotW+mphfSlotsTailPad))
	}
	layout := s.layout[:size]
	out.bits, s.ranks = rankDirectory(out.bits, s.ranks)
	placed := uint64(n - len(out.fallback))
	posMask := widthMask[h.plan.posW]
	var sum uint64
	for j, key := range s.hashes {
		var slot uint64
		if i, ok := findBit(out.bits, out.levels, key); ok {
			slot = rankOf(out.bits, s.ranks, i)
		} else {
			at, found := slices.BinarySearchFunc(out.fallback, bbFallback{hash: key, fp: s.fps[j]}, compareFallback)
			if !found {
				return fmt.Errorf("%w: key %#x has no slot", ErrMPHFLookupFailed, key)
			}
			slot = placed + uint64(at)
		}
		if slot >= uint64(n) {
			return fmt.Errorf("%w: slot %d of %d", ErrMPHFLookupFailed, slot, n)
		}
		at := slot * slotW
		binary.LittleEndian.PutUint64(layout[at:], s.fps[j])
		pos := layout[at+mphfFingerprintBytes:]
		binary.LittleEndian.PutUint64(pos, binary.LittleEndian.Uint64(pos)&^posMask|s.pos[j]&posMask)
		sum += positionMix(s.pos[j])
	}
	base := h.plan.bases[p]
	var want uint64
	for i := base; i < base+uint64(n); i++ {
		want += positionMix(i)
	}
	h.posSum.Add(sum)
	h.wantSum.Add(want)
	if _, err := h.slotsFile.WriteAt(layout[:uint64(n)*slotW], int64(mphfSlotsDataOff+base*slotW)); err != nil {
		return fmt.Errorf("write MPHF slots: %w", err)
	}

	return nil
}

// positionMix is splitmix64 of x: summed over a build's positions, it
// matches the sum over [0, n) only if they are each position once, but
// for a chance collision.
func positionMix(x uint64) uint64 {
	x += splitmixGamma
	x ^= x >> splitmixShift1
	x *= splitmixMul1
	x ^= x >> splitmixShift2
	x *= splitmixMul2

	return x ^ x>>splitmixShift3
}

// readSpill hands spill file sf's count records to fn: those in its
// file (opened by path if it is not open) in chunks of buf, whole
// records, then those still in its buf. It closes and removes the file
// and drops buf. Records other than count fail.
func readSpill(sf *spillFile, count uint64, buf []byte, fn func(recs []byte) error) error {
	tail := sf.buf
	sf.buf = nil
	inBuf := uint64(len(tail)) / spillRecordBytes
	if inBuf > count || sf.f == nil && sf.path == "" && inBuf != count {
		return fmt.Errorf("%w: MPHF spill holds %d of its %d keys in memory", ErrMPHFLookupFailed, inBuf, count)
	}
	if sf.f == nil && sf.path != "" {
		f, err := os.Open(sf.path)
		if err != nil {
			return fmt.Errorf("open MPHF spill: %w", err)
		}
		sf.f = f
	}
	if sf.f != nil {
		err := errors.Join(readRecords(sf.f, count-inBuf, buf, fn), sf.closeFile())
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(sf.path), err)
		}
		if err := os.Remove(sf.path); err != nil {
			return fmt.Errorf("remove MPHF spill: %w", err)
		}
	}
	if len(tail) == 0 {
		return nil
	}

	return fn(tail)
}

func readRecords(f *os.File, count uint64, buf []byte, fn func(recs []byte) error) error {
	var off int64
	for left := count * spillRecordBytes; left > 0; {
		chunk := buf[:min(uint64(len(buf)), left)]
		if _, err := f.ReadAt(chunk, off); err != nil {
			return fmt.Errorf("%w: read MPHF spill: %w", ErrMPHFLookupFailed, err)
		}
		if err := fn(chunk); err != nil {
			return err
		}
		off += int64(len(chunk))
		left -= uint64(len(chunk))
	}
	if n, _ := f.ReadAt(buf[:1], off); n != 0 {
		return fmt.Errorf("%w: MPHF spill holds more than its %d keys", ErrMPHFLookupFailed, count)
	}

	return nil
}

// split splits every spill file into its partitions' own files, on up
// to maxSplitters goroutines each holding a write buffer per partition
// of its bucket.
func (h *MPHFBuild) split() error {
	splitters := min(maxSplitters, h.maxWorkers)
	var next atomic.Int64
	errs := make([]error, splitters)
	var wg sync.WaitGroup
	for w := range splitters {
		wg.Go(func() {
			subs := make([]spillFile, 1<<(h.plan.bits-spillBits))
			slab := make([]byte, len(subs)*spillBufBytes)
			for i := range subs {
				subs[i].buf = slab[i*spillBufBytes : i*spillBufBytes : (i+1)*spillBufBytes]
			}
			read := make([]byte, spillReadBytes)
			for b := int(next.Add(1) - 1); b < spillBuckets && !h.stopAll.Load(); b = int(next.Add(1) - 1) {
				if errs[w] = h.splitBucket(b, subs, read); errs[w] != nil {
					return
				}
			}
		})
	}
	wg.Wait()

	return errors.Join(errs...)
}

// splitBucket writes spill file b's records to its partitions' files.
func (h *MPHFBuild) splitBucket(b int, subs []spillFile, read []byte) error {
	if h.plan.buckets[b] == 0 {
		return nil
	}
	dir := h.b.spill.dir
	shift := 64 - h.plan.bits
	first := b * len(subs)
	err := readSpill(&h.b.spill.files[b], h.plan.buckets[b], read, func(recs []byte) error {
		for r := 0; r < len(recs); r += spillRecordBytes {
			p := int(binary.LittleEndian.Uint64(recs[r:]) >> shift)
			sf := &subs[p-first]
			if err := sf.room(func() string { return partPath(dir, p) }); err != nil {
				return err
			}
			sf.buf = append(sf.buf, recs[r:r+spillRecordBytes]...)
		}

		return nil
	})
	for i := range subs {
		sf := &subs[i]
		if len(sf.buf) > 0 {
			p := first + i
			err = errors.Join(err, sf.writeOut(func() string { return partPath(dir, p) }))
		}
		if sf.f != nil {
			err = errors.Join(err, sf.closeFile())
		}
	}

	return err
}
