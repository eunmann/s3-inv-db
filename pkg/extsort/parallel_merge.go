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

	"github.com/eunmann/s3-inv-db/internal/logctx"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
)

// ParallelMergeConfig configures parallel merge operations.
type ParallelMergeConfig struct {
	// NumWorkers is the number of concurrent merge workers.
	// Default: max(1, NumCPU/2).
	NumWorkers int

	// MaxFanIn is the maximum number of runs merged in a single worker (K for K-way).
	// Higher values reduce merge rounds but increase memory per worker.
	// Default: 8.
	MaxFanIn int

	// BufferSize is the read/write buffer size per run file.
	// Default: 1MB.
	BufferSize int

	// TempDir is the directory for intermediate merge output.
	// If empty, uses os.TempDir().
	TempDir string

	// UseCompression determines whether to write compressed intermediate runs.
	// Default: true.
	UseCompression bool

	// CompressionLevel is the compression level for intermediate runs.
	// Default: CompressionFastest (optimize for merge speed).
	CompressionLevel CompressionLevel
}

// DefaultParallelMergeConfig returns sensible defaults for parallel merge.
func DefaultParallelMergeConfig() ParallelMergeConfig {
	numCPU := runtime.NumCPU()
	workers := numCPU / 2
	if workers < 1 {
		workers = 1
	}
	if workers > 8 {
		workers = 8 // Cap to avoid excessive parallelism
	}

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
	config   ParallelMergeConfig
	tempDir  string
	runCount atomic.Int64 // Counter for generating unique run file names

	// Statistics
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
		config:  config,
		tempDir: tempDir,
	}
}

// mergeJob represents a group of runs to merge.
type mergeJob struct {
	inputPaths []string
	outputPath string
	jobIndex   int
}

// mergeResult holds the result of a merge job.
type mergeResult struct {
	outputPath   string
	recordCount  uint64
	bytesWritten int64
	duration     time.Duration
	err          error
	jobIndex     int
}

// MergeAll merges all input run files into a single sorted output.
// Returns the path to the final merged run file.
// The caller is responsible for cleaning up the output file when done.
func (m *ParallelMerger) MergeAll(ctx context.Context, inputPaths []string) (string, error) {
	if len(inputPaths) == 0 {
		return "", errors.New("no input paths provided")
	}

	if len(inputPaths) == 1 {
		// Nothing to merge, just return the single input
		return inputPaths[0], nil
	}

	log := logctx.FromContext(ctx)
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
			// Clean up any files we created
			for _, p := range nextPaths {
				os.Remove(p)
			}
			return "", fmt.Errorf("merge round %d: %w", round, err)
		}

		// Clean up input files from previous round (except original inputs on first round)
		if round > 1 {
			for _, p := range currentPaths {
				os.Remove(p)
			}
		}

		currentPaths = nextPaths
		log.Info().
			Int("round", round).
			Int("output_files_count", len(currentPaths)).
			Msg("merge round complete")
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.mergeWorker(ctx, jobs, results)
		}()
	}

	// Send jobs
	for i, group := range groups {
		outputPath := filepath.Join(m.tempDir, fmt.Sprintf("merge_r%d_%04d.crun", round, m.runCount.Add(1)))
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
	var firstErr error

	for result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		outputPaths[result.jobIndex] = result.outputPath
		m.totalBytesWritten += result.bytesWritten
	}

	if firstErr != nil {
		// Clean up any successful outputs
		for _, p := range outputPaths {
			if p != "" {
				os.Remove(p)
			}
		}
		return nil, firstErr
	}

	return outputPaths, nil
}

// partitionPaths splits paths into groups of at most MaxFanIn.
func (m *ParallelMerger) partitionPaths(paths []string) [][]string {
	var groups [][]string
	for i := 0; i < len(paths); i += m.config.MaxFanIn {
		end := i + m.config.MaxFanIn
		if end > len(paths) {
			end = len(paths)
		}
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

// executeMerge performs a single K-way merge of input files to output file.
func (m *ParallelMerger) executeMerge(ctx context.Context, job mergeJob) mergeResult {
	startTime := time.Now()
	log := logctx.FromContext(ctx)

	result := mergeResult{
		outputPath: job.outputPath,
		jobIndex:   job.jobIndex,
	}

	// Open all input readers
	readers := make([]RunReader, 0, len(job.inputPaths))
	for _, path := range job.inputPaths {
		reader, err := OpenRunFileAuto(path, m.config.BufferSize)
		if err != nil {
			result.err = fmt.Errorf("open input %s: %w", path, err)
			for _, r := range readers {
				r.Close()
			}
			return result
		}
		readers = append(readers, reader)
	}

	// Create merge iterator using the existing heap-based merger
	merger, err := newMergeIteratorFromRunReaders(readers)
	if err != nil {
		result.err = fmt.Errorf("create merger: %w", err)
		for _, r := range readers {
			r.Close()
		}
		return result
	}

	// Create output writer
	var outputWriter interface {
		Write(row *PrefixRow) error
		Close() error
	}

	if m.config.UseCompression {
		w, err := NewCompressedRunWriter(job.outputPath, CompressedRunWriterOptions{
			BufferSize:       m.config.BufferSize,
			CompressionLevel: m.config.CompressionLevel,
		})
		if err != nil {
			result.err = fmt.Errorf("create output writer: %w", err)
			merger.Close()
			return result
		}
		outputWriter = w
	} else {
		w, err := NewRunFileWriter(job.outputPath, m.config.BufferSize)
		if err != nil {
			result.err = fmt.Errorf("create output writer: %w", err)
			merger.Close()
			return result
		}
		outputWriter = w
	}

	// Merge all records
	var count uint64
	for {
		select {
		case <-ctx.Done():
			outputWriter.Close()
			merger.Close()
			os.Remove(job.outputPath)
			result.err = ctx.Err()
			return result
		default:
		}

		row, err := merger.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			outputWriter.Close()
			merger.Close()
			os.Remove(job.outputPath)
			result.err = fmt.Errorf("read from merger: %w", err)
			return result
		}

		if err := outputWriter.Write(row); err != nil {
			outputWriter.Close()
			merger.Close()
			os.Remove(job.outputPath)
			result.err = fmt.Errorf("write to output: %w", err)
			return result
		}
		count++
	}

	// Close merger (which closes all readers)
	merger.Close()

	// Close output writer
	if err := outputWriter.Close(); err != nil {
		os.Remove(job.outputPath)
		result.err = fmt.Errorf("close output: %w", err)
		return result
	}

	// Get output file size
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
	readers []RunReader
	heap    *mergeHeap
	err     error
}

// newMergeIteratorFromRunReaders creates a merge iterator from RunReader interfaces.
func newMergeIteratorFromRunReaders(readers []RunReader) (*runReaderMergeIterator, error) {
	m := &runReaderMergeIterator{
		readers: readers,
		heap:    &mergeHeap{items: make([]mergeItem, 0, len(readers))},
	}

	for i, r := range readers {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			continue // empty reader
		}
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("initial read from run %d: %w", i, err)
		}
		m.heap.items = append(m.heap.items, mergeItem{row: row, readerIdx: i})
	}

	heapInit(m.heap)
	return m, nil
}

// Next returns the next merged PrefixRow in sorted order.
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

	// Merge duplicates
	for m.heap.Len() > 0 && m.heap.items[0].row.Prefix == result.Prefix {
		dup := heapPop(m.heap)
		result.Merge(dup.row)

		if err := m.advanceReader(dup.readerIdx); err != nil && !errors.Is(err, io.EOF) {
			m.err = err
			return nil, err
		}
	}

	return result, nil
}

// advanceReader reads the next row from the given reader and pushes to heap.
func (m *runReaderMergeIterator) advanceReader(idx int) error {
	row, err := m.readers[idx].Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return fmt.Errorf("advance reader %d: %w", idx, err)
	}
	heapPush(m.heap, mergeItem{row: row, readerIdx: idx})
	return nil
}

// Close closes all underlying readers.
func (m *runReaderMergeIterator) Close() error {
	var firstErr error
	for _, r := range m.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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

// Statistics returns merge statistics.
func (m *ParallelMerger) Statistics() (rounds int, totalTime time.Duration, bytesWritten int64) {
	return m.mergeRounds, m.totalMergeTime, m.totalBytesWritten
}

// CleanupIntermediateFiles removes all intermediate merge files from the temp directory.
// Call this after the final merge is complete and you've processed the output.
func (m *ParallelMerger) CleanupIntermediateFiles() error {
	pattern := filepath.Join(m.tempDir, "merge_r*_*.crun")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob intermediate files: %w", err)
	}

	var firstErr error
	for _, match := range matches {
		if err := os.Remove(match); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
