// Package events is the pipeline-wide pub-sub event bus.
//
// Each pipeline stage publishes structured Event values; subscribers
// receive them via a channel. Publishers never block on subscribers:
// if a subscriber's channel is full, the event is dropped for that
// subscriber and a Dropped counter is incremented. This keeps the
// hot ingest path from being held hostage by a slow consumer.
//
// Typical use:
//
//	bus := events.NewBus()
//	defer bus.Close()
//
//	sub := bus.Subscribe(events.StageAggregator, 1024)
//	go func() {
//	    for ev := range sub.C {
//	        // ...
//	    }
//	}()
//
//	bus.Publish(events.Event{
//	    Stage: events.StageAggregator,
//	    Type:  events.EvtBatchCommitted,
//	    Payload: events.BatchCommitted{Rows: 10_000, Bytes: 4_096_000},
//	})
//
// Zero-cost when nobody is subscribed: Publish returns immediately
// after a single atomic load + nil check.
package events

import (
	"sync"
	"sync/atomic"
	"time"
)

// Stage identifies which pipeline stage emitted an event. Add new
// stages here as the pipeline grows.
type Stage string

const (
	StagePipeline   Stage = "pipeline"
	StageDownload   Stage = "download"
	StageParse      Stage = "parse"
	StageAggregator Stage = "aggregator"
	StageSpill      Stage = "spill"
	StageMerge      Stage = "merge"
	StageIndexBuild Stage = "index_build"
	StageMPHF       Stage = "mphf"
)

// Type names a specific event within a stage.
type Type string

const (
	EvtStageStart      Type = "stage_start"
	EvtStageEnd        Type = "stage_end"
	EvtBatchCommitted  Type = "batch_committed"
	EvtSpillStarted    Type = "spill_started"
	EvtSpillCompleted  Type = "spill_completed"
	EvtWorkerIdle      Type = "worker_idle"
	EvtWorkerBusy      Type = "worker_busy"
	EvtRoundStarted    Type = "round_started"
	EvtRoundCompleted  Type = "round_completed"
	EvtFinalizeStarted Type = "finalize_started"
	EvtFinalizeEnded   Type = "finalize_ended"
)

// Event is the unit of communication on the bus. Payload type is
// up to the publisher; subscribers type-assert.
type Event struct {
	Stage   Stage
	Type    Type
	Time    time.Time
	Payload any
}

// Common payloads. Stages can also publish their own custom shapes.

// BatchCommitted reports rows/bytes processed by a stage in one
// commit (e.g. a chunk parsed, a spill written, a merge step).
type BatchCommitted struct {
	WorkerID int
	Rows     uint64
	Bytes    uint64
}

// SpillCompleted reports the result of a spill write.
type SpillCompleted struct {
	WorkerID   int
	Rows       uint64
	Bytes      int64
	Duration   time.Duration
	OutputPath string
}

// WorkerState reports a worker transitioning between idle and busy.
// Used to compute utilization.
type WorkerState struct {
	WorkerID int
	Reason   string // e.g. "waiting_jobs", "downloading", "parsing", "aggregating", "spilling"
}

// StageTiming reports a full stage start/end pair.
type StageTiming struct {
	Stage    Stage
	Duration time.Duration
	Rows     uint64
	Bytes    uint64
}

// Subscription is a handle for a subscriber. Receive from C; check
// Dropped for events that the bus couldn't deliver because the
// channel was full. Call Cancel to detach.
type Subscription struct {
	C       <-chan Event
	dropped atomic.Uint64
	stages  map[Stage]struct{} // empty = all stages
	send    chan<- Event
	cancel  func()
}

// Dropped returns how many events were dropped because this
// subscription's channel was full.
func (s *Subscription) Dropped() uint64 { return s.dropped.Load() }

// Cancel detaches the subscription. The C channel will be closed
// once the bus drops the reference. Safe to call multiple times.
func (s *Subscription) Cancel() {
	if s.cancel != nil {
		s.cancel()
	}
}

// Bus is the shared event bus. Zero-value is unusable; call NewBus.
type Bus struct {
	mu     sync.Mutex
	subs   []*Subscription
	closed atomic.Bool
}

// NewBus constructs an empty bus. Always returns a usable value.
func NewBus() *Bus {
	return &Bus{}
}

// Subscribe registers a subscriber for the given stages. Pass no
// stages to subscribe to all events. The returned Subscription owns
// a channel of the given buffer size; if events arrive faster than
// the subscriber reads, they're dropped (counted via Dropped()).
func (b *Bus) Subscribe(buffer int, stages ...Stage) *Subscription {
	if buffer < 1 {
		buffer = 1
	}
	ch := make(chan Event, buffer)
	sub := &Subscription{
		C:    ch,
		send: ch,
	}
	if len(stages) > 0 {
		sub.stages = make(map[Stage]struct{}, len(stages))
		for _, s := range stages {
			sub.stages[s] = struct{}{}
		}
	}
	sub.cancel = func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		for i, s := range b.subs {
			if s == sub {
				b.subs = append(b.subs[:i], b.subs[i+1:]...)
				close(ch)

				return
			}
		}
	}
	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.mu.Unlock()

	return sub
}

// Publish broadcasts an event to matching subscribers. Never
// blocks: if a subscriber's channel is full, the event is dropped
// for that subscriber. Returns immediately if the bus is closed.
//
// If Time is zero, set to time.Now().
func (b *Bus) Publish(ev Event) {
	if b == nil || b.closed.Load() {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	// Snapshot the subscriber list under the lock. We never call
	// into a subscriber while holding the lock — a slow channel
	// receive on send would deadlock with Cancel.
	b.mu.Lock()
	subs := b.subs
	if len(subs) == 0 {
		b.mu.Unlock()

		return
	}
	snapshot := make([]*Subscription, len(subs))
	copy(snapshot, subs)
	b.mu.Unlock()

	for _, sub := range snapshot {
		if sub.stages != nil {
			if _, ok := sub.stages[ev.Stage]; !ok {
				continue
			}
		}
		select {
		case sub.send <- ev:
		default:
			sub.dropped.Add(1)
		}
	}
}

// Close detaches all subscribers and disables future Publish calls.
// Safe to call multiple times.
func (b *Bus) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	b.mu.Lock()
	subs := b.subs
	b.subs = nil
	b.mu.Unlock()
	for _, sub := range subs {
		close(sub.send)
	}
}

// HasSubscribers reports whether the bus has any subscribers. Hot
// publishers can guard expensive payload construction with this.
func (b *Bus) HasSubscribers() bool {
	if b == nil || b.closed.Load() {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.subs) > 0
}
