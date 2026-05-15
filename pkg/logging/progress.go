package logging

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/rs/zerolog"
)

// percentScale converts the [0..1] fraction (done/total) to a [0..100]
// percentage. Pulled out as a named constant so mnd doesn't flag it
// each time we render progress.
const percentScale = 100.0

// progressRecentWindow is the size of the moving-average window the
// tracker keeps for ETA smoothing.
const progressRecentWindow = 10

// ProgressTracker tracks progress for a set of items with ETA calculation.
// It is safe for concurrent use.
type ProgressTracker struct {
	total     int64
	completed atomic.Int64
	skipped   atomic.Int64
	startTime time.Time
	log       zerolog.Logger
	phase     string

	// For moving average of item durations.
	mu              sync.Mutex
	recentDurations []time.Duration
	maxRecent       int
}

// NewProgressTracker creates a new progress tracker.
func NewProgressTracker(phase string, total int64, log zerolog.Logger) *ProgressTracker {
	return &ProgressTracker{
		total:           total,
		startTime:       time.Now(),
		log:             log,
		phase:           phase,
		recentDurations: make([]time.Duration, 0, progressRecentWindow),
		maxRecent:       progressRecentWindow,
	}
}

// RecordCompletion records that an item completed with the given duration.
func (pt *ProgressTracker) RecordCompletion(d time.Duration) {
	pt.completed.Add(1)

	pt.mu.Lock()
	if len(pt.recentDurations) >= pt.maxRecent {
		pt.recentDurations = pt.recentDurations[1:]
	}
	pt.recentDurations = append(pt.recentDurations, d)
	pt.mu.Unlock()
}

// RecordSkip records that an item was skipped.
func (pt *ProgressTracker) RecordSkip() {
	pt.skipped.Add(1)
}

// ProgressSnapshot reports the tracker's counts at a point in time.
// Replaces the previous (completed, skipped, total int64) named-return
// signature so progress can be read as a struct instead of three
// positional ints.
type ProgressSnapshot struct {
	Completed int64
	Skipped   int64
	Total     int64
}

// Progress returns current progress stats as a snapshot.
func (pt *ProgressTracker) Progress() ProgressSnapshot {
	return ProgressSnapshot{
		Completed: pt.completed.Load(),
		Skipped:   pt.skipped.Load(),
		Total:     pt.total,
	}
}

// ProgressPct returns the progress percentage (0-100).
func (pt *ProgressTracker) ProgressPct() float64 {
	done := pt.completed.Load() + pt.skipped.Load()
	if pt.total == 0 {
		return percentScale
	}

	return float64(done) * percentScale / float64(pt.total)
}

// ETA returns the estimated time remaining based on average completion rate.
func (pt *ProgressTracker) ETA() time.Duration {
	completed := pt.completed.Load()
	if completed == 0 {
		return 0
	}

	remaining := pt.total - completed - pt.skipped.Load()
	if remaining <= 0 {
		return 0
	}

	// Use moving average if available, else overall average.
	pt.mu.Lock()
	var avgDuration time.Duration
	if len(pt.recentDurations) > 0 {
		var sum time.Duration
		for _, d := range pt.recentDurations {
			sum += d
		}
		avgDuration = sum / time.Duration(len(pt.recentDurations))
	} else {
		elapsed := time.Since(pt.startTime)
		avgDuration = elapsed / time.Duration(completed)
	}
	pt.mu.Unlock()

	return avgDuration * time.Duration(remaining)
}

// Elapsed returns time since tracking started.
func (pt *ProgressTracker) Elapsed() time.Duration {
	return time.Since(pt.startTime)
}

// Remaining returns how many items are remaining.
func (pt *ProgressTracker) Remaining() int64 {
	return pt.total - pt.completed.Load() - pt.skipped.Load()
}

// Completed returns only the completed count (not skipped).
func (pt *ProgressTracker) Completed() int64 {
	return pt.completed.Load()
}

// Total returns the total count.
func (pt *ProgressTracker) Total() int64 {
	return pt.total
}

// CompletionEvent helps build consistent completion log events.
type CompletionEvent struct {
	log     zerolog.Logger
	event   string
	phase   string
	elapsed time.Duration
	pretty  bool
	fields  map[string]any
}

// NewCompletionEvent creates a new completion event builder. The pretty
// flag mirrors the LogEvent contract: when true, *_h human-readable
// companion fields are emitted alongside the raw values.
func NewCompletionEvent(log zerolog.Logger, event, phase string, elapsed time.Duration, pretty bool) *CompletionEvent {
	return &CompletionEvent{
		log:     log,
		event:   event,
		phase:   phase,
		elapsed: elapsed,
		pretty:  pretty,
		fields:  make(map[string]any),
	}
}

// Str adds a string field.
func (ce *CompletionEvent) Str(key, val string) *CompletionEvent {
	ce.fields[key] = val

	return ce
}

// Int adds an int field.
func (ce *CompletionEvent) Int(key string, val int) *CompletionEvent {
	ce.fields[key] = val

	return ce
}

// Int64 adds an int64 field.
func (ce *CompletionEvent) Int64(key string, val int64) *CompletionEvent {
	ce.fields[key] = val

	return ce
}

// Uint64 adds a uint64 field.
func (ce *CompletionEvent) Uint64(key string, val uint64) *CompletionEvent {
	ce.fields[key] = val

	return ce
}

// Float64 adds a float64 field.
func (ce *CompletionEvent) Float64(key string, val float64) *CompletionEvent {
	ce.fields[key] = val

	return ce
}

// Bytes adds byte count with optional human-readable companion.
func (ce *CompletionEvent) Bytes(key string, bytes int64) *CompletionEvent {
	ce.fields[key] = bytes
	if ce.pretty {
		ce.fields[key+"_h"] = humanfmt.Bytes(bytes)
	}

	return ce
}

// BytesUint64 adds a uint64 byte count field.
func (ce *CompletionEvent) BytesUint64(key string, bytes uint64) *CompletionEvent {
	return ce.Bytes(key, int64(bytes))
}

// Count adds count with optional human-readable companion.
func (ce *CompletionEvent) Count(key string, n int64) *CompletionEvent {
	ce.fields[key] = n
	if ce.pretty {
		ce.fields[key+"_h"] = humanfmt.Count(n)
	}

	return ce
}

// CountUint64 adds a uint64 count field.
func (ce *CompletionEvent) CountUint64(key string, n int64) *CompletionEvent {
	return ce.Count(key, n)
}

// Progress adds progress fields (done, total, percentage, optional ETA).
func (ce *CompletionEvent) Progress(done, total int64, eta time.Duration) *CompletionEvent {
	ce.fields["done"] = done
	ce.fields["total"] = total
	if total > 0 {
		pct := float64(done) * percentScale / float64(total)
		ce.fields["progress_pct"] = pct
		if ce.pretty {
			ce.fields["progress_h"] = humanfmt.Count(done) + "/" + humanfmt.Count(total)
		}
	}
	if eta > 0 {
		ce.fields["eta_ms"] = eta.Milliseconds()
		if ce.pretty {
			ce.fields["eta_h"] = humanfmt.Duration(eta)
		}
	}

	return ce
}

// ProgressFromTracker adds progress fields from a ProgressTracker.
func (ce *CompletionEvent) ProgressFromTracker(pt *ProgressTracker) *CompletionEvent {
	snap := pt.Progress()
	done := snap.Completed + snap.Skipped
	ce.fields["completed"] = snap.Completed
	ce.fields["skipped"] = snap.Skipped
	ce.fields["total"] = snap.Total
	if snap.Total > 0 {
		pct := float64(done) * percentScale / float64(snap.Total)
		ce.fields["progress_pct"] = pct
	}
	if eta := pt.ETA(); eta > 0 {
		ce.fields["eta_ms"] = eta.Milliseconds()
		if ce.pretty {
			ce.fields["eta_h"] = humanfmt.Duration(eta)
		}
	}

	return ce
}

// Throughput adds throughput fields.
func (ce *CompletionEvent) Throughput(bytes int64) *CompletionEvent {
	if ce.elapsed > 0 {
		bps := float64(bytes) / ce.elapsed.Seconds()
		ce.fields["throughput_bps"] = bps
		if ce.pretty {
			ce.fields["throughput_h"] = humanfmt.Throughput(bytes, ce.elapsed)
		}
	}

	return ce
}

// Log emits the completion event.
func (ce *CompletionEvent) Log(msg string) {
	e := ce.log.Info().
		Str("event", ce.event).
		Str("phase", ce.phase).
		Int64("duration_ms", ce.elapsed.Milliseconds())

	if ce.pretty {
		e = e.Str("duration_h", humanfmt.Duration(ce.elapsed))
	}

	for k, v := range ce.fields {
		e = e.Interface(k, v)
	}

	e.Msg(msg)
}

// LogDebug emits the completion event at debug level.
func (ce *CompletionEvent) LogDebug(msg string) {
	e := ce.log.Debug().
		Str("event", ce.event).
		Str("phase", ce.phase).
		Int64("duration_ms", ce.elapsed.Milliseconds())

	if ce.pretty {
		e = e.Str("duration_h", humanfmt.Duration(ce.elapsed))
	}

	for k, v := range ce.fields {
		e = e.Interface(k, v)
	}

	e.Msg(msg)
}

// PhaseComplete logs a phase completion event.
func PhaseComplete(log zerolog.Logger, phase string, elapsed time.Duration, pretty bool) *CompletionEvent {
	return NewCompletionEvent(log, "phase_completed", phase, elapsed, pretty)
}

// ChunkComplete logs a chunk completion event.
func ChunkComplete(log zerolog.Logger, phase string, elapsed time.Duration, pretty bool) *CompletionEvent {
	return NewCompletionEvent(log, "chunk_completed", phase, elapsed, pretty)
}

// BatchComplete logs a batch/transaction completion event.
func BatchComplete(log zerolog.Logger, phase string, elapsed time.Duration, pretty bool) *CompletionEvent {
	return NewCompletionEvent(log, "batch_completed", phase, elapsed, pretty)
}

// FileCreated logs a file creation completion event.
func FileCreated(log zerolog.Logger, phase string, elapsed time.Duration, pretty bool) *CompletionEvent {
	return NewCompletionEvent(log, "file_created", phase, elapsed, pretty)
}

// ChunkStarted logs a chunk start event (no duration, no progress_pct).
func ChunkStarted(log zerolog.Logger, phase, chunkID string, chunksComplete, chunksTotal int64) {
	log.Info().
		Str("event", "chunk_started").
		Str("phase", phase).
		Str("chunk_id", chunkID).
		Int64("chunks_complete", chunksComplete).
		Int64("chunks_total", chunksTotal).
		Msg("chunk started")
}
