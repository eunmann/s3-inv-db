package extsort

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/rs/zerolog"
)

// ParallelMergeConfig configures parallel merge operations. Zero
// NumWorkers and MaxFanIn take DefaultConfig's merge settings.
type ParallelMergeConfig struct {
	OnRoundComplete func(round, remainingFiles int)
	TempDir         string
	NumWorkers      int
	// MaxFanIn caps the runs one disk-round merge job reads.
	MaxFanIn int
	// FinalFanIn caps the runs the final streaming merge reads; disk
	// rounds run only while there are more. At least MaxFanIn.
	FinalFanIn       int
	BufferSize       int
	CompressionLevel CompressionLevel
}

// mergeReaderBufferSize is each merge reader's and writer's I/O buffer.
const mergeReaderBufferSize = 1 << 20

// DefaultFinalFanIn is how many runs the final streaming merge reads
// at once. An open run costs its read buffer plus a zstd decoder, a few
// MiB, and the final merge runs after ingest has released its
// aggregators, so it can take far more runs than a disk round: a build
// whose every ingest worker spilled once (one run per CPU) merges
// straight into the index with no disk round.
const DefaultFinalFanIn = 64

// ParallelMerger coordinates parallel merging of sorted run files.
// Statistics-tracking fields are stored as atomics so Statistics() is
// safe to call from any goroutine without requiring the caller to
// arrange a happens-before barrier against the merge driver.
type ParallelMerger struct {
	config              ParallelMergeConfig
	tempDir             string
	instanceID          string
	runCount            atomic.Int64
	totalMergeTimeNanos atomic.Int64
	totalBytesWritten   atomic.Int64
	mergeRounds         atomic.Int64
}

// NewParallelMerger creates a new parallel merger with the given configuration.
func NewParallelMerger(config ParallelMergeConfig) *ParallelMerger {
	merge := MergeConfig{NumWorkers: config.NumWorkers, MaxFanIn: config.MaxFanIn}.withDefaults()
	config.NumWorkers, config.MaxFanIn = merge.NumWorkers, merge.MaxFanIn
	if config.BufferSize <= 0 {
		config.BufferSize = mergeReaderBufferSize
	}
	if config.FinalFanIn <= 0 {
		config.FinalFanIn = DefaultFinalFanIn
	}
	config.FinalFanIn = max(config.FinalFanIn, config.MaxFanIn)

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
// Disk rounds run only while there are more than FinalFanIn runs; the
// final merge is a live K-way MergeIterator that the IndexBuilder
// consumes directly, so the deduplicated prefix set is never written
// out and read back.
//
// The merger owns the input files: each is deleted as soon as it has
// been merged into a larger run, and the cleanup closure closes the
// iterator and deletes the runs it read. On error every remaining run
// is deleted.
func (m *ParallelMerger) MergeAllToIterator(ctx context.Context, inputPaths []string) (RowIterator, func() error, error) {
	if len(inputPaths) == 0 {
		return nil, func() error { return nil }, ErrNoInputPaths
	}

	log := zerolog.Ctx(ctx)
	startTime := time.Now()

	paths := slices.Clone(inputPaths)
	for round := 1; len(paths) > m.config.FinalFanIn; round++ {
		if err := ctx.Err(); err != nil {
			removeFiles(paths)

			return nil, nil, fmt.Errorf("merge cancelled: %w", err)
		}
		m.mergeRounds.Store(int64(round))
		plan := m.planRound(paths)
		outputs, err := m.mergeRound(ctx, plan.groups, round)
		if err != nil {
			removeFiles(paths)

			return nil, nil, fmt.Errorf("merge round %d: %w", round, err)
		}
		paths = slices.Concat(plan.kept, outputs)
		if m.config.OnRoundComplete != nil {
			m.config.OnRoundComplete(round, len(paths))
		}
	}

	iter, err := NewMergeIterator(paths, m.config.BufferSize)
	if err != nil {
		removeFiles(paths)

		return nil, nil, fmt.Errorf("open final merge iterator: %w", err)
	}

	totalMergeTime := time.Since(startTime)
	m.totalMergeTimeNanos.Store(int64(totalMergeTime))
	log.Info().
		Int64("rounds_count", m.mergeRounds.Load()).
		Int("final_fan_in", len(paths)).
		Str("disk_rounds_duration", humanfmt.Duration(totalMergeTime)).
		Msg("parallel merge → streaming iterator")

	cleanup := func() error {
		err := iter.Close()
		removeFiles(paths)

		return err
	}

	return iter, cleanup, nil
}

// roundPlan is one disk round: the groups to merge, each into one new
// run, and the runs left as they are.
type roundPlan struct {
	groups [][]string
	kept   []string
}

// planRound picks a disk round's merges. When one round can bring the
// run count down to FinalFanIn, it merges only the smallest runs that
// takes, in as few groups as possible, and leaves the rest of the data
// unwritten. Otherwise it merges every run, split evenly into groups of
// at most MaxFanIn. Either way no group holds a single run.
func (m *ParallelMerger) planRound(paths []string) roundPlan {
	fanIn := m.config.MaxFanIn
	// Merging a group of g runs into one removes g-1 of them.
	excess := len(paths) - m.config.FinalFanIn
	numGroups := (excess + fanIn - 2) / (fanIn - 1)
	if merged := excess + numGroups; merged <= len(paths) {
		bySize := sortBySize(paths)

		return roundPlan{groups: splitEvenly(bySize[:merged], numGroups), kept: bySize[merged:]}
	}

	return roundPlan{groups: splitEvenly(paths, (len(paths)+fanIn-1)/fanIn)}
}

// sortBySize returns paths ordered by file size, smallest first. A run
// that cannot be stat'd sorts first; opening it will report the error.
func sortBySize(paths []string) []string {
	sizes := make(map[string]int64, len(paths))
	for _, p := range paths {
		if info, err := os.Stat(p); err == nil {
			sizes[p] = info.Size()
		}
	}
	sorted := slices.Clone(paths)
	slices.SortStableFunc(sorted, func(a, b string) int { return cmp.Compare(sizes[a], sizes[b]) })

	return sorted
}

// splitEvenly splits paths into n groups whose sizes differ by at most one.
func splitEvenly(paths []string, n int) [][]string {
	groups := make([][]string, 0, n)
	for i := range n {
		groups = append(groups, paths[i*len(paths)/n:(i+1)*len(paths)/n])
	}

	return groups
}

// mergeRound merges each group into one new run, in parallel, and
// returns the new runs in group order. A merged group's inputs are
// deleted as soon as its output is complete.
func (m *ParallelMerger) mergeRound(ctx context.Context, groups [][]string, round int) ([]string, error) {
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
		m.totalBytesWritten.Add(result.bytesWritten)
	}

	if len(errs) > 0 {
		// Clean up any successful outputs
		log := logging.L()
		for _, p := range outputPaths {
			if p != "" {
				logCleanupErr(log, "remove abandoned merge output", p, os.Remove(p))
			}
		}

		return nil, errors.Join(errs...)
	}

	return outputPaths, nil
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

// openInputReaders opens all input run files, closing any successfully
// opened readers if one fails.
func (m *ParallelMerger) openInputReaders(inputPaths []string) ([]*CompressedRunReader, error) {
	readers := make([]*CompressedRunReader, 0, len(inputPaths))
	for _, path := range inputPaths {
		reader, err := OpenCompressedRunFile(path, m.config.BufferSize)
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

// createMergeOutputWriter creates the run writer for a merge job's output.
func (m *ParallelMerger) createMergeOutputWriter(outputPath string) (*CompressedRunWriter, error) {
	w, err := NewCompressedRunWriter(outputPath, CompressedRunWriterOptions{
		BufferSize:       m.config.BufferSize,
		CompressionLevel: m.config.CompressionLevel,
	})
	if err != nil {
		return nil, fmt.Errorf("create output writer: %w", err)
	}

	return w, nil
}

// drainMerger pulls every row from merger and writes it to outputWriter,
// aborting promptly when ctx is cancelled. The caller still owns merger
// and outputWriter on return.
func drainMerger(ctx context.Context, merger *MergeIterator, outputWriter *CompressedRunWriter) (uint64, error) {
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
			return count, fmt.Errorf("write to output: %w", err)
		}
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

	merger, err := newMergeIterator(readers)
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
		log := logging.L()
		outputWriter.Close()
		merger.Close()
		logCleanupErr(log, "remove failed merge output", job.outputPath, os.Remove(job.outputPath))
		result.err = err

		return result
	}

	merger.Close()

	if err := outputWriter.Close(); err != nil {
		log := logging.L()
		logCleanupErr(log, "remove failed merge output", job.outputPath, os.Remove(job.outputPath))
		result.err = fmt.Errorf("close output: %w", err)

		return result
	}

	if info, err := os.Stat(job.outputPath); err == nil {
		result.bytesWritten = info.Size()
	}
	for _, p := range job.inputPaths {
		logCleanupErr(log, "remove merged run", p, os.Remove(p))
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

// MergeStatistics describes the work done by a ParallelMerger.
type MergeStatistics struct {
	Rounds         int
	TotalMergeTime time.Duration
	BytesWritten   int64
}

// Statistics returns merge statistics. The fields are stored as atomics
// so this method is safe to call from any goroutine, including before
// MergeAllToIterator has returned (it will report whatever has been
// accumulated so far).
func (m *ParallelMerger) Statistics() MergeStatistics {
	return MergeStatistics{
		Rounds:         int(m.mergeRounds.Load()),
		TotalMergeTime: time.Duration(m.totalMergeTimeNanos.Load()),
		BytesWritten:   m.totalBytesWritten.Load(),
	}
}
