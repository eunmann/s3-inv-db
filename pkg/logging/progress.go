package logging

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/rs/zerolog"
)

// ProgressTracker tracks progress for a set of items with ETA calculation.
// It is safe for concurrent use.
type ProgressTracker struct {
	total     int64
	completed atomic.Int64
	skipped   atomic.Int64
	startTime time.Time
	log       zerolog.Logger
	phase     string

	// For moving average of item durations
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
		recentDurations: make([]time.Duration, 0, 10),
		maxRecent:       10,
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

// Progress returns current progress stats.
func (pt *ProgressTracker) Progress() (completed, skipped, total int64) {
	return pt.completed.Load(), pt.skipped.Load(), pt.total
}

// ProgressPct returns the progress percentage (0-100).
func (pt *ProgressTracker) ProgressPct() float64 {
	done := pt.completed.Load() + pt.skipped.Load()
	if pt.total == 0 {
		return 100.0
	}
	return float64(done) * 100.0 / float64(pt.total)
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

	// Use moving average if available, else overall average
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
	fields  map[string]interface{}
}

// NewCompletionEvent creates a new completion event builder.
func NewCompletionEvent(log zerolog.Logger, event, phase string, elapsed time.Duration) *CompletionEvent {
	return &CompletionEvent{
		log:     log,
		event:   event,
		phase:   phase,
		elapsed: elapsed,
		fields:  make(map[string]interface{}),
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
	if IsPrettyMode() {
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
	if IsPrettyMode() {
		ce.fields[key+"_h"] = humanfmt.Count(n)
	}
	return ce
}

// CountUint64 adds a uint64 count field.
func (ce *CompletionEvent) CountUint64(key string, n uint64) *CompletionEvent {
	return ce.Count(key, int64(n))
}

// Progress adds progress fields (done, total, percentage, optional ETA).
func (ce *CompletionEvent) Progress(done, total int64, eta time.Duration) *CompletionEvent {
	ce.fields["done"] = done
	ce.fields["total"] = total
	if total > 0 {
		pct := float64(done) * 100.0 / float64(total)
		ce.fields["progress_pct"] = pct
		if IsPrettyMode() {
			ce.fields["progress_h"] = humanfmt.Count(done) + "/" + humanfmt.Count(total)
		}
	}
	if eta > 0 {
		ce.fields["eta_ms"] = eta.Milliseconds()
		if IsPrettyMode() {
			ce.fields["eta_h"] = humanfmt.Duration(eta)
		}
	}
	return ce
}

// ProgressFromTracker adds progress fields from a ProgressTracker.
func (ce *CompletionEvent) ProgressFromTracker(pt *ProgressTracker) *CompletionEvent {
	completed, skipped, total := pt.Progress()
	done := completed + skipped
	ce.fields["completed"] = completed
	ce.fields["skipped"] = skipped
	ce.fields["total"] = total
	if total > 0 {
		pct := float64(done) * 100.0 / float64(total)
		ce.fields["progress_pct"] = pct
	}
	if eta := pt.ETA(); eta > 0 {
		ce.fields["eta_ms"] = eta.Milliseconds()
		if IsPrettyMode() {
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
		if IsPrettyMode() {
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

	if IsPrettyMode() {
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

	if IsPrettyMode() {
		e = e.Str("duration_h", humanfmt.Duration(ce.elapsed))
	}

	for k, v := range ce.fields {
		e = e.Interface(k, v)
	}

	e.Msg(msg)
}

// PhaseComplete logs a phase completion event.
func PhaseComplete(log zerolog.Logger, phase string, elapsed time.Duration) *CompletionEvent {
	return NewCompletionEvent(log, "phase_completed", phase, elapsed)
}

// ChunkComplete logs a chunk completion event.
func ChunkComplete(log zerolog.Logger, phase string, elapsed time.Duration) *CompletionEvent {
	return NewCompletionEvent(log, "chunk_completed", phase, elapsed)
}

// BatchComplete logs a batch/transaction completion event.
func BatchComplete(log zerolog.Logger, phase string, elapsed time.Duration) *CompletionEvent {
	return NewCompletionEvent(log, "batch_completed", phase, elapsed)
}

// FileCreated logs a file creation completion event.
func FileCreated(log zerolog.Logger, phase string, elapsed time.Duration) *CompletionEvent {
	return NewCompletionEvent(log, "file_created", phase, elapsed)
}

// ChunkStarted logs a chunk start event (no duration, no progress_pct).
func ChunkStarted(log zerolog.Logger, phase string, chunkID string, chunksComplete, chunksTotal int64) {
	log.Info().
		Str("event", "chunk_started").
		Str("phase", phase).
		Str("chunk_id", chunkID).
		Int64("chunks_complete", chunksComplete).
		Int64("chunks_total", chunksTotal).
		Msg("chunk started")
}
