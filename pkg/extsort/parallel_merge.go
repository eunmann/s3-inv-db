package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/rs/zerolog"
)

// ParallelMergeConfig configures parallel merge operations.
type ParallelMergeConfig struct {
	OnRoundComplete  func(round, remainingFiles int)
	TempDir          string
	NumWorkers       int
	MaxFanIn         int
	BufferSize       int
	CompressionLevel CompressionLevel
	UseCompression   bool
}

// DefaultParallelMergeConfig returns sensible defaults for parallel merge.
func DefaultParallelMergeConfig() ParallelMergeConfig {
	numCPU := runtime.NumCPU()
	workers := min(max(numCPU/2, 1),
		// Cap to avoid excessive parallelism
		8)

	return ParallelMergeConfig{
		NumWorkers:       workers,
		MaxFanIn:         16,              // Higher fan-in reduces merge rounds, each reader uses ~1MB
		BufferSize:       1 * 1024 * 1024, // 1MB
		UseCompression:   true,
		CompressionLevel: CompressionFastest,
	}
}

// ParallelMerger coordinates parallel merging of sorted run files.
type ParallelMerger struct {
	config            ParallelMergeConfig
	tempDir           string
	instanceID        string
	runCount          atomic.Int64
	totalMergeTime    time.Duration
	totalBytesWritten int64
	mergeRounds       int
}

// NewParallelMerger creates a new parallel merger with the given configuration.
func NewParallelMerger(config ParallelMergeConfig) *ParallelMerger {
	if config.NumWorkers <= 0 {
		config.NumWorkers = DefaultParallelMergeConfig().NumWorkers
	}
	if config.MaxFanIn <= 1 {
		config.MaxFanIn = DefaultParallelMergeConfig().MaxFanIn
	}
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultParallelMergeConfig().BufferSize
	}

	tempDir := config.TempDir
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	return &ParallelMerger{
		config:     config,
		tempDir:    tempDir,
		instanceID: newMergerInstanceID(),
	}
}

// nextMergerInstanceCounter is a process-wide counter that
// disambiguates concurrent ParallelMerger instances. Combined with the
// process PID it gives intermediate files unique names within the
// shared tempDir.
//
//nolint:gochecknoglobals // process-wide counter, by design
var nextMergerInstanceCounter atomic.Int64

func newMergerInstanceID() string {
	return fmt.Sprintf("p%d_i%d", os.Getpid(), nextMergerInstanceCounter.Add(1))
}

// mergeJob represents a group of runs to merge.
type mergeJob struct {
	outputPath string
	inputPaths []string
	jobIndex   int
}

// mergeResult holds the result of a merge job.
type mergeResult struct {
	err          error
	outputPath   string
	recordCount  uint64
	bytesWritten int64
	duration     time.Duration
	jobIndex     int
}

// MergeAllToIterator merges all input run files and returns a
// streaming row iterator over the final sorted output, plus a
// cleanup closure the caller must run when done.
//
// Unlike MergeAll, no final-merged file is written to disk: when the
// remaining file count fits in MaxFanIn after the N-1 disk-backed
// rounds, the last round is replaced by a live K-way MergeIterator
// that the IndexBuilder consumes directly. At billion-prefix scale
// this saves writing and re-reading the entire deduplicated prefix
// set (hundreds of GB of avoidable I/O).
//
// The cleanup closure closes the iterator's underlying readers and
// removes any intermediate files this call produced. It does NOT
// remove the original input files; that remains the caller's job.
//
//nolint:ireturn // RowIterator is the package's iterator contract; consumers don't need the concrete type.
func (m *ParallelMerger) MergeAllToIterator(ctx context.Context, inputPaths []string) (RowIterator, func() error, error) {
	if len(inputPaths) == 0 {
		return nil, func() error { return nil }, ErrNoInputPaths
	}

	log := zerolog.Ctx(ctx)
	startTime := time.Now()

	finalPaths := inputPaths
	round := 0
	intermediates := []string{}

	// Disk-backed rounds while the file count exceeds MaxFanIn —
	// each round shrinks the count by ~MaxFanIn×.
	for len(finalPaths) > m.config.MaxFanIn {
		select {
		case <-ctx.Done():
			removeFiles(intermediates)

			return nil, nil, fmt.Errorf("merge cancelled: %w", ctx.Err())
		default:
		}
		round++
		m.mergeRounds = round
		nextPaths, err := m.mergeRound(ctx, finalPaths, round)
		if err != nil {
			removeFiles(append(intermediates, nextPaths...))

			return nil, nil, fmt.Errorf("merge round %d: %w", round, err)
		}
		if round > 1 {
			removeFiles(finalPaths)
		}
		intermediates = append(intermediates, nextPaths...)
		finalPaths = nextPaths
		if m.config.OnRoundComplete != nil {
			m.config.OnRoundComplete(round, len(finalPaths))
		}
	}

	// Final round: stream the K-way merge directly, no file write.
	iter, err := NewMergeIterator(finalPaths, m.config.BufferSize)
	if err != nil {
		removeFiles(intermediates)

		return nil, nil, fmt.Errorf("open final merge iterator: %w", err)
	}

	m.totalMergeTime = time.Since(startTime)
	log.Info().
		Int("rounds_count", m.mergeRounds).
		Int("final_fan_in", len(finalPaths)).
		Str("disk_rounds_duration", humanfmt.Duration(m.totalMergeTime)).
		Msg("parallel merge → streaming iterator")

	cleanup := func() error {
		err := iter.Close()
		removeFiles(intermediates)

		return err
	}

	return iter, cleanup, nil
}

// MergeAll merges all input run files into a single sorted output.
// Returns the path to the final merged run file.
// The caller is responsible for cleaning up the output file when done.
func (m *ParallelMerger) MergeAll(ctx context.Context, inputPaths []string) (string, error) {
	if len(inputPaths) == 0 {
		return "", ErrNoInputPaths
	}

	if len(inputPaths) == 1 {
		// Nothing to merge, just return the single input
		return inputPaths[0], nil
	}

	log := zerolog.Ctx(ctx)
	startTime := time.Now()

	currentPaths := inputPaths
	round := 0

	// Multi-round merge until we have a single file
	for len(currentPaths) > 1 {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("merge cancelled: %w", ctx.Err())
		default:
		}

		round++
		m.mergeRounds = round

		log.Info().
			Int("round", round).
			Int("input_files_count", len(currentPaths)).
			Int("workers_count", m.config.NumWorkers).
			Int("max_fan_in", m.config.MaxFanIn).
			Msg("starting merge round")

		nextPaths, err := m.mergeRound(ctx, currentPaths, round)
		if err != nil {
			removeFiles(nextPaths)

			return "", fmt.Errorf("merge round %d: %w", round, err)
		}

		// Clean up input files from previous round (except original inputs on first round)
		if round > 1 {
			removeFiles(currentPaths)
		}

		currentPaths = nextPaths
		log.Info().
			Int("round", round).
			Int("output_files_count", len(currentPaths)).
			Msg("merge round complete")
		if m.config.OnRoundComplete != nil {
			m.config.OnRoundComplete(round, len(currentPaths))
		}
	}

	m.totalMergeTime = time.Since(startTime)

	log.Info().
		Int("rounds_count", m.mergeRounds).
		Str("total_duration", humanfmt.Duration(m.totalMergeTime)).
		Dur("total_duration_ms", m.totalMergeTime).
		Msg("parallel merge complete")

	return currentPaths[0], nil
}

// mergeRound performs one round of parallel merging.
// Groups input paths by MaxFanIn and merges each group in parallel.
func (m *ParallelMerger) mergeRound(ctx context.Context, inputPaths []string, round int) ([]string, error) {
	// Partition inputs into groups of MaxFanIn
	groups := m.partitionPaths(inputPaths)

	// Create job channel and result channel
	jobs := make(chan mergeJob, len(groups))
	results := make(chan mergeResult, len(groups))

	// Start workers
	var wg sync.WaitGroup
	for range min(m.config.NumWorkers, len(groups)) {
		wg.Go(func() {
			m.mergeWorker(ctx, jobs, results)
		})
	}

	// Send jobs
	for i, group := range groups {
		outputPath := filepath.Join(m.tempDir, fmt.Sprintf("merge_%s_r%d_%04d.crun", m.instanceID, round, m.runCount.Add(1)))
		jobs <- mergeJob{
			inputPaths: group,
			outputPath: outputPath,
			jobIndex:   i,
		}
	}
	close(jobs)

	// Wait for workers to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	outputPaths := make([]string, len(groups))
	var errs []error

	for result := range results {
		if result.err != nil {
			errs = append(errs, result.err)

			continue
		}
		outputPaths[result.jobIndex] = result.outputPath
		m.totalBytesWritten += result.bytesWritten
	}

	if len(errs) > 0 {
		// Clean up any successful outputs
		for _, p := range outputPaths {
			if p != "" {
				os.Remove(p)
			}
		}

		return nil, errors.Join(errs...)
	}

	return outputPaths, nil
}

// partitionPaths splits paths into groups of at most MaxFanIn.
func (m *ParallelMerger) partitionPaths(paths []string) [][]string {
	var groups [][]string
	for i := 0; i < len(paths); i += m.config.MaxFanIn {
		end := min(i+m.config.MaxFanIn, len(paths))
		groups = append(groups, paths[i:end])
	}

	return groups
}

// mergeWorker processes merge jobs from the jobs channel.
func (m *ParallelMerger) mergeWorker(ctx context.Context, jobs <-chan mergeJob, results chan<- mergeResult) {
	for job := range jobs {
		select {
		case <-ctx.Done():
			results <- mergeResult{
				err:      ctx.Err(),
				jobIndex: job.jobIndex,
			}

			return
		default:
		}

		result := m.executeMerge(ctx, job)
		results <- result
	}
}

// mergeOutputWriter is the minimal interface satisfied by both the
// compressed and uncompressed run file writers.
type mergeOutputWriter interface {
	Write(row *PrefixRow) error
	Close() error
}

// openInputReaders opens all input run files, closing any successfully
// opened readers if one fails.
func (m *ParallelMerger) openInputReaders(inputPaths []string) ([]RunReader, error) {
	readers := make([]RunReader, 0, len(inputPaths))
	for _, path := range inputPaths {
		reader, err := OpenRunFileAuto(path, m.config.BufferSize)
		if err != nil {
			for _, r := range readers {
				r.Close()
			}

			return nil, fmt.Errorf("open input %s: %w", path, err)
		}
		readers = append(readers, reader)
	}

	return readers, nil
}

// createMergeOutputWriter creates the appropriate output writer based on
// the merger's compression configuration.
func (m *ParallelMerger) createMergeOutputWriter(outputPath string) (mergeOutputWriter, error) {
	if m.config.UseCompression {
		w, err := NewCompressedRunWriter(outputPath, CompressedRunWriterOptions{
			BufferSize:       m.config.BufferSize,
			CompressionLevel: m.config.CompressionLevel,
		})
		if err != nil {
			return nil, fmt.Errorf("create output writer: %w", err)
		}

		return w, nil
	}
	w, err := NewRunFileWriter(outputPath, m.config.BufferSize)
	if err != nil {
		return nil, fmt.Errorf("create output writer: %w", err)
	}

	return w, nil
}

// drainMerger pulls every row from merger and writes it to outputWriter,
// aborting promptly when ctx is cancelled. The caller still owns merger
// and outputWriter on return.
func drainMerger(ctx context.Context, merger *runReaderMergeIterator, outputWriter mergeOutputWriter) (uint64, error) {
	var count uint64
	for {
		select {
		case <-ctx.Done():
			return count, fmt.Errorf("merge cancelled: %w", ctx.Err())
		default:
		}

		row, err := merger.Next()
		if errors.Is(err, io.EOF) {
			return count, nil
		}
		if err != nil {
			return count, fmt.Errorf("read from merger: %w", err)
		}

		if err := outputWriter.Write(row); err != nil {
			merger.Release(row)

			return count, fmt.Errorf("write to output: %w", err)
		}
		merger.Release(row)
		count++
	}
}

// executeMerge performs a single K-way merge of input files to output file.
func (m *ParallelMerger) executeMerge(ctx context.Context, job mergeJob) mergeResult {
	startTime := time.Now()
	log := zerolog.Ctx(ctx)

	result := mergeResult{
		outputPath: job.outputPath,
		jobIndex:   job.jobIndex,
	}

	readers, err := m.openInputReaders(job.inputPaths)
	if err != nil {
		result.err = err

		return result
	}

	merger, err := newMergeIteratorFromRunReaders(readers)
	if err != nil {
		result.err = fmt.Errorf("create merger: %w", err)
		for _, r := range readers {
			r.Close()
		}

		return result
	}

	outputWriter, err := m.createMergeOutputWriter(job.outputPath)
	if err != nil {
		result.err = err
		merger.Close()

		return result
	}

	count, err := drainMerger(ctx, merger, outputWriter)
	if err != nil {
		outputWriter.Close()
		merger.Close()
		os.Remove(job.outputPath)
		result.err = err

		return result
	}

	merger.Close()

	if err := outputWriter.Close(); err != nil {
		os.Remove(job.outputPath)
		result.err = fmt.Errorf("close output: %w", err)

		return result
	}

	if info, err := os.Stat(job.outputPath); err == nil {
		result.bytesWritten = info.Size()
	}

	result.recordCount = count
	result.duration = time.Since(startTime)

	log.Debug().
		Int("job_index", job.jobIndex).
		Int("input_files_count", len(job.inputPaths)).
		Uint64("records_count", count).
		Str("bytes_written", humanfmt.Bytes(result.bytesWritten)).
		Str("duration", humanfmt.Duration(result.duration)).
		Msg("merge job complete")

	return result
}

// runReaderMergeIterator wraps RunReader interface for use with merge heap.
type runReaderMergeIterator struct {
	err     error
	heap    *mergeHeap
	readers []RunReader
}

// mergeRowPool reuses PrefixRow allocations across the merge phase
// (billion-row builds otherwise churn hundreds of GiB).
//
//nolint:gochecknoglobals // package-level pool, by design
var mergeRowPool = newTypedPool(func() *PrefixRow { return &PrefixRow{} })

func acquireMergeRow() *PrefixRow {
	r := mergeRowPool.Get()
	r.Reset()

	return r
}

// releaseMergeRow returns a row to the pool. Callers of
// runReaderMergeIterator.Next must release the returned row when
// they're done with its data — the next call to Next may overwrite
// it via the pool.
func releaseMergeRow(r *PrefixRow) {
	if r == nil {
		return
	}
	mergeRowPool.Put(r)
}

// newMergeIteratorFromRunReaders creates a merge iterator from RunReader interfaces.
func newMergeIteratorFromRunReaders(readers []RunReader) (*runReaderMergeIterator, error) {
	m := &runReaderMergeIterator{
		readers: readers,
		heap:    &mergeHeap{items: make([]mergeItem, 0, len(readers))},
	}

	for i, r := range readers {
		row := acquireMergeRow()
		err := r.ReadInto(row)
		if errors.Is(err, io.EOF) {
			releaseMergeRow(row)

			continue // empty reader
		}
		if err != nil {
			releaseMergeRow(row)
			m.Close()

			return nil, fmt.Errorf("initial read from run %d: %w", i, err)
		}
		m.heap.items = append(m.heap.items, mergeItem{row: row, readerIdx: i})
	}

	heapInit(m.heap)

	return m, nil
}

// Next returns the next merged PrefixRow in sorted order. The caller
// owns the returned row until it calls Release(row); after Release the
// row may be handed to a future Next caller via the pool.
func (m *runReaderMergeIterator) Next() (*PrefixRow, error) {
	if m.err != nil {
		return nil, m.err
	}

	if m.heap.Len() == 0 {
		return nil, io.EOF
	}

	item := heapPop(m.heap)
	result := item.row

	if err := m.advanceReader(item.readerIdx); err != nil && !errors.Is(err, io.EOF) {
		m.err = err

		return nil, err
	}

	// Merge duplicates. The duplicate rows are returned to the pool
	// after their data is folded into result.
	for m.heap.Len() > 0 && m.heap.items[0].row.Prefix == result.Prefix {
		dup := heapPop(m.heap)
		result.Merge(dup.row)
		releaseMergeRow(dup.row)

		if err := m.advanceReader(dup.readerIdx); err != nil && !errors.Is(err, io.EOF) {
			m.err = err

			return nil, err
		}
	}

	return result, nil
}

// Release returns a row received from Next back to the pool. Callers
// must not retain the row after calling Release.
func (m *runReaderMergeIterator) Release(row *PrefixRow) {
	releaseMergeRow(row)
}

// Remaining returns 0 — RunReaders don't surface a known total ahead
// of time. Satisfies the RowIterator interface for the builder's
// capacity hint.
func (m *runReaderMergeIterator) Remaining() uint64 { return 0 }

// advanceReader reads the next row from the given reader and pushes to heap.
func (m *runReaderMergeIterator) advanceReader(idx int) error {
	row := acquireMergeRow()
	err := m.readers[idx].ReadInto(row)
	if err != nil {
		releaseMergeRow(row)
		if errors.Is(err, io.EOF) {
			return io.EOF
		}

		return fmt.Errorf("advance reader %d: %w", idx, err)
	}
	heapPush(m.heap, mergeItem{row: row, readerIdx: idx})

	return nil
}

// Close closes all underlying readers, joining any errors.
func (m *runReaderMergeIterator) Close() error {
	errs := make([]error, 0, len(m.readers))
	for _, r := range m.readers {
		if err := r.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// Heap operations without using container/heap to avoid interface{} conversions

func heapInit(h *mergeHeap) {
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		heapDown(h, i, n)
	}
}

func heapPush(h *mergeHeap, item mergeItem) {
	h.items = append(h.items, item)
	heapUp(h, h.Len()-1)
}

func heapPop(h *mergeHeap) mergeItem {
	n := h.Len() - 1
	h.items[0], h.items[n] = h.items[n], h.items[0]
	heapDown(h, 0, n)
	item := h.items[n]
	h.items = h.items[:n]

	return item
}

func heapUp(h *mergeHeap, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func heapDown(h *mergeHeap, i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.items[j2].row.Prefix < h.items[j1].row.Prefix {
			j = j2 // = 2*i + 2  // right child
		}
		if h.items[i].row.Prefix <= h.items[j].row.Prefix {
			break
		}
		h.Swap(i, j)
		i = j
	}
}

// MergeStatistics describes the work done by a ParallelMerger.
type MergeStatistics struct {
	Rounds         int
	TotalMergeTime time.Duration
	BytesWritten   int64
}

// Statistics returns merge statistics. Must be called only after
// MergeAll / MergeAllToIterator returns: the fields are written by
// the merge caller goroutine and have no synchronisation barrier
// against concurrent readers.
func (m *ParallelMerger) Statistics() MergeStatistics {
	return MergeStatistics{
		Rounds:         m.mergeRounds,
		TotalMergeTime: m.totalMergeTime,
		BytesWritten:   m.totalBytesWritten,
	}
}

// CleanupIntermediateFiles removes all intermediate merge files from the temp directory.
// Call this after the final merge is complete and you've processed the output.
func (m *ParallelMerger) CleanupIntermediateFiles() error {
	// Glob only this merger instance's files so a concurrent merger
	// in the same tempDir doesn't have its files swept by our cleanup.
	pattern := filepath.Join(m.tempDir, fmt.Sprintf("merge_%s_r*_*.crun", m.instanceID))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob intermediate files: %w", err)
	}

	errs := make([]error, 0, len(matches))
	for _, match := range matches {
		if err := os.Remove(match); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
