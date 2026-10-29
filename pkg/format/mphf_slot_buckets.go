package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
)

// A prefix's MPHF slot is a hash of it, random relative to its position,
// so filling mph_slots.bin in slot order by reading each slot's
// fingerprint by position reads the fingerprint array at random: once
// the array is out of the page cache, every slot is a synchronous page
// fault. Slot buckets keep every pass sequential instead. The slots are
// split into ranges (buckets); one pass in position order looks up each
// prefix's slot and appends its (slot, fingerprint, position) record to
// its bucket's region of one scratch file; then each bucket's records
// are read in sequence, laid out in memory and written to the slot file
// with one pwrite.
//
// The MPHF maps the n prefixes onto slots [0, n) one to one, so bucket
// b receives exactly as many records as it has slots: its region's
// offset and size are known before the pass, and a region that
// overflows or ends short is a lookup bug the build fails on.
//
// A bucket's slot count is a power of two, so a slot's bucket is a shift
// and its place in the bucket a mask rather than a 64-bit division per
// prefix.
const (
	// SlotBucketMinBytes is the least slot-file range a bucket covers.
	// Rounded up to a power-of-two slot count, it sizes a write
	// worker's layout buffer below maxSlotBuckets buckets: up to twice
	// this.
	slotBucketMinBytes = 1 << 20
	// MaxSlotBuckets caps the bucket count, so the append buffers stay
	// bounded whatever the prefix count: past it, buckets grow instead.
	maxSlotBuckets = 1024
	// SlotAppendBudget is a worker's append buffers across its buckets:
	// each bucket's buffer is this split over the buckets, between
	// slotAppendMin and slotAppendMax. A build with few buckets then
	// flushes in large writes, one with many in 4 KiB writes, and a
	// worker holds at most maxSlotBuckets × slotAppendMin.
	slotAppendBudget = 256 << 10
	slotAppendMin    = 4 << 10
	slotAppendMax    = 256 << 10
	// SlotWriteWorkers lay out and write buckets, each with one layout
	// buffer. A fixed count, not one per CPU: laying out a bucket is a
	// short scatter within one buffer, so two workers keep the writes
	// going, and every further one is another layout buffer on a heap
	// GOMEMLIMIT does not govern.
	slotWriteWorkers = 2
	// SlotReadBuffer is the read size within a bucket's region.
	slotReadBuffer = 256 << 10
)

// errTooManySlots means a bucket would hold more slots than a record's
// 4-byte slot field counts: past ~4.4 trillion prefixes.
var errTooManySlots = errors.New("too many MPHF slots")

// slotBuckets is the bucketed (slot, fingerprint, position) records of
// every prefix, in a scratch file in the build's temp dir.
type slotBuckets struct {
	file      *os.File
	path      string
	next      []atomic.Int64 // bytes appended to each bucket's region
	n         int            // slots
	perBucket int            // slots per bucket; the last may have fewer
	shift     uint           // log2 of perBucket
	appendCap int            // bytes of a worker's buffer per bucket, whole records
	recW      int            // record bytes: slot in bucket, fingerprint, position
	posBytes  int            // 4, or 8 past 2^32 prefixes
}

// newSlotBuckets sizes the buckets for n slots of slotW bytes and
// creates the scratch file: a bucket's slots are the fewest that cover
// slotBucketMinBytes and leave at most maxSlotBuckets buckets, rounded
// up to a power of two.
func newSlotBuckets(tempDir string, n, slotW int) (*slotBuckets, error) {
	need := max((slotBucketMinBytes+slotW-1)/slotW, (n+maxSlotBuckets-1)/maxSlotBuckets)
	shift := uint(bits.Len(uint(need - 1)))
	perBucket := 1 << shift
	if perBucket-1 > math.MaxUint32 {
		// A record holds its slot within the bucket in 4 bytes.
		return nil, fmt.Errorf("%w: %d slots per bucket", errTooManySlots, perBucket)
	}
	s := &slotBuckets{n: n, perBucket: perBucket, shift: shift, posBytes: 4}
	if uint64(n) > 1<<32 {
		s.posBytes = 8
	}
	s.recW = 4 + mphfFingerprintBytes + s.posBytes
	s.next = make([]atomic.Int64, (n+perBucket-1)/perBucket)
	s.appendCap = min(max(slotAppendBudget/len(s.next), slotAppendMin), slotAppendMax) / s.recW * s.recW
	f, err := os.CreateTemp(tempDir, "mphf_slot_buckets_*.tmp")
	if err != nil {
		return nil, fmt.Errorf("slot buckets: %w", err)
	}
	s.file, s.path = f, f.Name()

	return s, nil
}

// slots returns the number of slots in bucket b.
func (s *slotBuckets) slots(b int) int {
	return min(s.perBucket, s.n-b*s.perBucket)
}

// regionEnd returns the end offset of bucket b's region.
func (s *slotBuckets) regionEnd(b int) int64 {
	return int64(b*s.perBucket+s.slots(b)) * int64(s.recW)
}

// appendRecords writes buf, whole records of bucket b, to the next free
// part of the bucket's region.
func (s *slotBuckets) appendRecords(b int, buf []byte) error {
	end := s.next[b].Add(int64(len(buf)))
	off := int64(b*s.perBucket)*int64(s.recW) + end - int64(len(buf))
	if off+int64(len(buf)) > s.regionEnd(b) {
		return fmt.Errorf("%w: slot bucket %d received more records than it has slots", ErrMPHFLookupFailed, b)
	}
	if _, err := s.file.WriteAt(buf, off); err != nil {
		return fmt.Errorf("write slot bucket %d: %w", b, err)
	}

	return nil
}

// place looks up every prefix's slot, in position order over the
// contiguous ranges of mphfWorkerCount(n) workers, through the fallback
// table for keys the levels miss, and appends its record to its
// bucket. Placed counts the prefixes.
func (s *slotBuckets) place(mph *bbhash, hashes, fps []uint64, placed *stepCounter) error {
	errs := make([]error, mphfWorkerCount(s.n))
	chunk := (s.n + len(errs) - 1) / len(errs)
	var wg sync.WaitGroup
	for w := range errs {
		start, end := w*chunk, min((w+1)*chunk, s.n)
		if start >= end {
			continue
		}
		wg.Go(func() { errs[w] = s.placeRange(mph, hashes, fps, start, end, placed) })
	}
	wg.Wait()

	return errors.Join(errs...)
}

// placeRange places prefixes [start, end).
func (s *slotBuckets) placeRange(mph *bbhash, hashes, fps []uint64, start, end int, placed *stepCounter) error {
	// One allocation for every bucket's buffer: a worker appends to all
	// of them, since slots are random relative to positions.
	slab := make([]byte, len(s.next)*s.appendCap)
	bufs := make([][]byte, len(s.next))
	for b := range bufs {
		bufs[b] = slab[b*s.appendCap : b*s.appendCap : (b+1)*s.appendCap]
	}
	for i := start; i < end; i++ {
		slot, ok := mph.find(hashes[i])
		if !ok {
			slot, ok = mph.findFallback(hashes[i], fps[i])
		}
		if !ok || slot >= uint64(s.n) {
			return fmt.Errorf("%w: slot %d (found %t) for prefix %d", ErrMPHFLookupFailed, slot, ok, i)
		}
		b := int(slot >> s.shift)
		bufs[b] = s.appendRecord(bufs[b], int(slot)&(s.perBucket-1), fps[i], uint64(i))
		if len(bufs[b]) == cap(bufs[b]) {
			if err := s.appendRecords(b, bufs[b]); err != nil {
				return err
			}
			bufs[b] = bufs[b][:0]
		}
		if (i-start+1)%progressBlock == 0 {
			placed.add(progressBlock)
		}
	}
	placed.add((end - start) % progressBlock)
	for b, buf := range bufs {
		if len(buf) > 0 {
			if err := s.appendRecords(b, buf); err != nil {
				return err
			}
		}
	}

	return nil
}

// appendRecord appends one record to buf.
func (s *slotBuckets) appendRecord(buf []byte, slotInBucket int, fp, pos uint64) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(slotInBucket))
	buf = binary.LittleEndian.AppendUint64(buf, fp)
	if s.posBytes == 4 {
		return binary.LittleEndian.AppendUint32(buf, uint32(pos))
	}

	return binary.LittleEndian.AppendUint64(buf, pos)
}

// write lays out every bucket's slots and writes them to f, the slot
// file, whose slots start at mphfSlotsDataOff and are slotW bytes: the
// fingerprint, then the position at posW bytes. Up to slotWriteWorkers
// workers take buckets in turn, each with one layout buffer. Written
// counts the slots.
func (s *slotBuckets) write(f *os.File, posW int, written *stepCounter) error {
	for b := range s.next {
		if got, want := s.next[b].Load(), s.regionEnd(b)-int64(b*s.perBucket)*int64(s.recW); got != want {
			return fmt.Errorf("%w: slot bucket %d holds %d of its %d records", ErrMPHFLookupFailed, b, got/int64(s.recW), want/int64(s.recW))
		}
	}
	slotW := mphfFingerprintBytes + posW
	layoutBytes := s.perBucket * slotW
	workers := min(slotWriteWorkers, len(s.next))
	var next atomic.Int64
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			layout := make([]byte, layoutBytes+layoutSlack)
			read := make([]byte, slotReadBuffer/s.recW*s.recW)
			for b := int(next.Add(1) - 1); b < len(s.next); b = int(next.Add(1) - 1) {
				if err := s.writeBucket(f, b, slotW, layout, read); err != nil {
					errs[w] = err

					return
				}
				written.add(s.slots(b))
			}
		})
	}
	wg.Wait()

	return errors.Join(errs...)
}

// layoutSlack is the bytes past a layout buffer's last slot that a
// position's 8-byte store reaches.
const layoutSlack = 8

// writeBucket lays out bucket b's slots in layout from its records and
// writes them to the slot file. Each record takes two 8-byte stores, its
// fingerprint and its position. The position's store also covers the
// first 8-posW bytes of the next slot, which records in position order
// may have laid out already, so it merges into them: those bytes keep
// their value, laid out or not, and the next slot's fingerprint store
// writes them whole.
func (s *slotBuckets) writeBucket(f *os.File, b, slotW int, layout, read []byte) error {
	slots := s.slots(b)
	posMask := widthMask[slotW-mphfFingerprintBytes]
	off := int64(b*s.perBucket) * int64(s.recW)
	for remaining := int64(slots * s.recW); remaining > 0; {
		chunk := read[:min(int64(len(read)), remaining)]
		if _, err := s.file.ReadAt(chunk, off); err != nil {
			return fmt.Errorf("read slot bucket %d: %w", b, err)
		}
		for r := 0; r < len(chunk); r += s.recW {
			at := int(binary.LittleEndian.Uint32(chunk[r:])) * slotW
			fp := binary.LittleEndian.Uint64(chunk[r+4:])
			var pos uint64
			if s.posBytes == 4 {
				pos = uint64(binary.LittleEndian.Uint32(chunk[r+12:]))
			} else {
				pos = binary.LittleEndian.Uint64(chunk[r+12:])
			}
			binary.LittleEndian.PutUint64(layout[at:], fp)
			p := layout[at+mphfFingerprintBytes:]
			binary.LittleEndian.PutUint64(p, binary.LittleEndian.Uint64(p)&^posMask|pos&posMask)
		}
		off += int64(len(chunk))
		remaining -= int64(len(chunk))
	}
	fileOff := int64(mphfSlotsDataOff) + int64(b*s.perBucket)*int64(slotW)
	if _, err := f.WriteAt(layout[:slots*slotW], fileOff); err != nil {
		return fmt.Errorf("write MPHF slots of bucket %d: %w", b, err)
	}

	return nil
}

// close closes and removes the scratch file.
func (s *slotBuckets) close() error {
	return errors.Join(s.file.Close(), removeIfErr(s.path))
}
