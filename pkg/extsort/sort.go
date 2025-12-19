// Package extsort implements external sorting for large datasets.
package extsort

import (
	"bufio"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/inventory"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Config holds configuration for external sorting.
type Config struct {
	// MaxRecordsPerChunk is the maximum number of records to hold in memory.
	MaxRecordsPerChunk int
	// TmpDir is the directory for temporary run files.
	TmpDir string
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		MaxRecordsPerChunk: 1_000_000,
		TmpDir:             os.TempDir(),
	}
}

// Iterator provides sorted records from a merge.
type Iterator interface {
	// Next advances to the next record. Returns false when done.
	Next() bool
	// Record returns the current record. Valid after Next() returns true.
	Record() inventory.Record
	// Err returns any error encountered during iteration.
	Err() error
	// Close releases resources.
	Close() error
}

// Sorter performs external merge sort on inventory records.
type Sorter struct {
	cfg          Config
	runFiles     []string
	runNum       int
	totalRecords int64
}

// NewSorter creates a new external sorter.
func NewSorter(cfg Config) *Sorter {
	log := logging.WithPhase("sort_runs")
	log.Debug().
		Int("max_records_per_chunk", cfg.MaxRecordsPerChunk).
		Str("tmp_dir", cfg.TmpDir).
		Msg("created external sorter")
	return &Sorter{cfg: cfg}
}

// AddRecords adds records from an inventory reader, creating sorted run files.
func (s *Sorter) AddRecords(ctx context.Context, reader inventory.Reader) error {
	log := logging.WithPhase("sort_runs")
	chunk := make([]inventory.Record, 0, s.cfg.MaxRecordsPerChunk)
	recordsInFile := int64(0)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		rec, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read record: %w", err)
		}

		chunk = append(chunk, rec)
		recordsInFile++

		if len(chunk) >= s.cfg.MaxRecordsPerChunk {
			if err := s.flushChunk(chunk); err != nil {
				return fmt.Errorf("flush chunk: %w", err)
			}
			chunk = chunk[:0]
		}
	}

	// Flush remaining records
	if len(chunk) > 0 {
		if err := s.flushChunk(chunk); err != nil {
			return fmt.Errorf("flush final chunk: %w", err)
		}
	}

	s.totalRecords += recordsInFile
	log.Debug().
		Int64("records_in_file", recordsInFile).
		Int64("total_records", s.totalRecords).
		Int("runs_created", len(s.runFiles)).
		Msg("processed inventory file")

	return nil
}

// flushChunk sorts a chunk and writes it as a run file.
func (s *Sorter) flushChunk(chunk []inventory.Record) error {
	log := logging.WithPhase("sort_runs")
	runStart := time.Now()

	// Sort by key
	sort.Slice(chunk, func(i, j int) bool {
		return chunk[i].Key < chunk[j].Key
	})

	// Write run file
	runPath := filepath.Join(s.cfg.TmpDir, fmt.Sprintf("run_%06d.tsv", s.runNum))
	runIndex := s.runNum
	s.runNum++

	f, err := os.Create(runPath)
	if err != nil {
		return fmt.Errorf("create run file: %w", err)
	}

	w := bufio.NewWriter(f)
	for _, rec := range chunk {
		// Format: key\tsize\ttierID\n
		if _, err := fmt.Fprintf(w, "%s\t%d\t%d\n", rec.Key, rec.Size, rec.TierID); err != nil {
			f.Close()
			os.Remove(runPath)
			return fmt.Errorf("write record: %w", err)
		}
	}

	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(runPath)
		return fmt.Errorf("flush run file: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(runPath)
		return fmt.Errorf("close run file: %w", err)
	}

	s.runFiles = append(s.runFiles, runPath)

	log.Info().
		Int("run_index", runIndex).
		Int("records_in_run", len(chunk)).
		Dur("elapsed", time.Since(runStart)).
		Msg("created sort run")

	return nil
}

// Merge returns an iterator over all records in sorted order.
func (s *Sorter) Merge(ctx context.Context) (Iterator, error) {
	log := logging.WithPhase("merge_runs")

	if len(s.runFiles) == 0 {
		log.Info().Msg("no runs to merge")
		return &emptyIterator{}, nil
	}

	log.Info().
		Int("run_count", len(s.runFiles)).
		Int64("estimated_records", s.totalRecords).
		Msg("starting k-way merge")

	// Open all run files
	readers := make([]*runReader, 0, len(s.runFiles))
	for _, path := range s.runFiles {
		rr, err := newRunReader(path)
		if err != nil {
			// Close already opened readers
			for _, r := range readers {
				r.Close()
			}
			log.Error().Err(err).Str("path", path).Msg("failed to open run file")
			return nil, fmt.Errorf("open run file %s: %w", path, err)
		}
		readers = append(readers, rr)
	}

	return newMergeIterator(ctx, readers, s.totalRecords), nil
}

// Cleanup removes all temporary run files.
func (s *Sorter) Cleanup() error {
	var firstErr error
	for _, path := range s.runFiles {
		if err := os.Remove(path); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.runFiles = nil
	return firstErr
}

// runReader reads records from a sorted run file.
type runReader struct {
	file    *os.File
	scanner *bufio.Scanner
	current inventory.Record
	hasNext bool
	err     error
}

func newRunReader(path string) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	scanner := bufio.NewScanner(f)
	// Set a large buffer for long keys
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	rr := &runReader{
		file:    f,
		scanner: scanner,
	}
	rr.advance()
	return rr, nil
}

func (r *runReader) advance() {
	if r.err != nil {
		return
	}

	if !r.scanner.Scan() {
		r.hasNext = false
		r.err = r.scanner.Err()
		return
	}

	line := r.scanner.Text()
	parts := strings.SplitN(line, "\t", 3)
	if len(parts) < 2 {
		r.err = fmt.Errorf("malformed run line: %q", line)
		r.hasNext = false
		return
	}

	size, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		r.err = fmt.Errorf("invalid size in run line: %w", err)
		r.hasNext = false
		return
	}

	rec := inventory.Record{Key: parts[0], Size: size}

	// Parse tier ID if present (for backwards compatibility with old run files)
	if len(parts) >= 3 && parts[2] != "" {
		tierID, err := strconv.ParseUint(parts[2], 10, 8)
		if err != nil {
			r.err = fmt.Errorf("invalid tierID in run line: %w", err)
			r.hasNext = false
			return
		}
		rec.TierID = tiers.ID(tierID)
	}

	r.current = rec
	r.hasNext = true
}

func (r *runReader) Current() inventory.Record {
	return r.current
}

func (r *runReader) HasNext() bool {
	return r.hasNext
}

func (r *runReader) Next() {
	r.advance()
}

func (r *runReader) Close() error {
	return r.file.Close()
}

// mergeIterator performs k-way merge using a min-heap.
type mergeIterator struct {
	ctx              context.Context
	readers          []*runReader
	h                *mergeHeap
	current          inventory.Record
	err              error
	recordsMerged    int64
	estimatedRecords int64
	startTime        time.Time
	lastLogTime      time.Time
}

func newMergeIterator(ctx context.Context, readers []*runReader, estimatedRecords int64) *mergeIterator {
	h := &mergeHeap{}
	heap.Init(h)

	// Add initial elements from each reader
	for i, r := range readers {
		if r.HasNext() {
			heap.Push(h, &heapItem{record: r.Current(), readerIdx: i})
		}
	}

	return &mergeIterator{
		ctx:              ctx,
		readers:          readers,
		h:                h,
		estimatedRecords: estimatedRecords,
		startTime:        time.Now(),
		lastLogTime:      time.Now(),
	}
}

func (m *mergeIterator) Next() bool {
	if m.err != nil {
		return false
	}

	if m.ctx.Err() != nil {
		m.err = m.ctx.Err()
		return false
	}

	if m.h.Len() == 0 {
		return false
	}

	// Pop smallest
	item := heap.Pop(m.h).(*heapItem)
	m.current = item.record
	m.recordsMerged++

	// Log progress every 5 seconds
	if time.Since(m.lastLogTime) >= 5*time.Second {
		log := logging.WithPhase("merge_runs")
		log.Info().
			Int64("records_merged", m.recordsMerged).
			Int64("estimated_total", m.estimatedRecords).
			Dur("elapsed", time.Since(m.startTime)).
			Msg("merge progress")
		m.lastLogTime = time.Now()
	}

	// Advance the reader that provided this item
	r := m.readers[item.readerIdx]
	r.Next()
	if r.err != nil {
		m.err = r.err
		return false
	}
	if r.HasNext() {
		heap.Push(m.h, &heapItem{record: r.Current(), readerIdx: item.readerIdx})
	}

	return true
}

func (m *mergeIterator) Record() inventory.Record {
	return m.current
}

func (m *mergeIterator) Err() error {
	return m.err
}

func (m *mergeIterator) Close() error {
	log := logging.WithPhase("merge_runs")
	log.Info().
		Int64("records_merged", m.recordsMerged).
		Dur("elapsed", time.Since(m.startTime)).
		Msg("merge complete")

	var firstErr error
	for _, r := range m.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// heapItem represents an item in the merge heap.
type heapItem struct {
	record    inventory.Record
	readerIdx int
}

// mergeHeap implements heap.Interface for k-way merge.
type mergeHeap []*heapItem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].record.Key < h[j].record.Key }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(*heapItem))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// emptyIterator returns no records.
type emptyIterator struct{}

func (e *emptyIterator) Next() bool               { return false }
func (e *emptyIterator) Record() inventory.Record { return inventory.Record{} }
func (e *emptyIterator) Err() error               { return nil }
func (e *emptyIterator) Close() error             { return nil }
