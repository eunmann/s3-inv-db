package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/rs/zerolog"
)

// Work is the user's code for one job. It runs on a background
// goroutine. The report function posts progress updates back to
// subscribers. Returning an error transitions the job to failed;
// returning nil with no ctx cancellation transitions to succeeded.
type Work func(ctx context.Context, report func(Update)) error

// ErrNotFound is returned by Cancel for unknown job IDs.
var ErrNotFound = errors.New("job not found")

// ErrShutdown is returned by Submit after Shutdown has been called.
var ErrShutdown = errors.New("job manager is shut down")

// ErrDuplicateInventory is returned by Submit when a live (queued or
// running) job already exists for the inventory. The returned Job
// carries the existing job's ID so callers can surface or join it.
var ErrDuplicateInventory = errors.New("inventory already has a live job")

// defaultMaxConcurrency bounds simultaneously-running jobs when no
// WithMaxConcurrency option is given. Builds are CPU- and disk-heavy and
// already parallelise internally, so serialising them is the safe default.
const defaultMaxConcurrency = 1

// jobStore is the persistence sink Scheduler writes job snapshots to.
// Defaulted to a no-op so tests can construct a Scheduler without a
// real database.
type jobStore interface {
	Upsert(ctx context.Context, j Job) error
}

type noopJobStore struct{}

func (noopJobStore) Upsert(context.Context, Job) error { return nil }

// Scheduler owns the in-memory registry of live jobs and their cancel
// handles. Job snapshots are persisted to the attached store on every
// transition and broadcast on Bus so the SSE handler can push updates
// to the UI. When no store is wired (via SetStore or NewScheduler's store
// arg) persistence is silently dropped.
type Scheduler struct {
	logger          zerolog.Logger
	store           jobStore
	bus             *Bus
	cancels         map[ID]context.CancelFunc
	liveByInventory map[inventory.ID]ID
	sem             chan struct{}
	maxConcurrency  int
	wg              sync.WaitGroup
	mu              sync.Mutex
	shutdown        bool
}

// Option configures a Scheduler at construction. See WithLogger.
type Option func(*Scheduler)

// WithLogger installs the Scheduler's background logger; without it,
// background goroutine errors that can't surface to a caller are
// suppressed (zerolog.Nop()).
func WithLogger(l zerolog.Logger) Option {
	return func(s *Scheduler) { s.logger = l }
}

// WithMaxConcurrency caps how many jobs run work simultaneously. Excess
// jobs stay queued until a slot frees. N <= 0 falls back to the default.
func WithMaxConcurrency(n int) Option {
	return func(s *Scheduler) {
		if n > 0 {
			s.maxConcurrency = n
		}
	}
}

// NewScheduler wires a Store and a Bus. A nil store is replaced with a
// no-op so tests don't need a SQLite handle.
func NewScheduler(store *Store, bus *Bus, opts ...Option) *Scheduler {
	s := &Scheduler{
		bus:             bus,
		cancels:         make(map[ID]context.CancelFunc),
		liveByInventory: make(map[inventory.ID]ID),
		logger:          zerolog.Nop(),
		store:           noopJobStore{},
		maxConcurrency:  defaultMaxConcurrency,
	}
	if store != nil {
		s.store = store
	}
	for _, opt := range opts {
		opt(s)
	}
	s.sem = make(chan struct{}, s.maxConcurrency)

	return s
}

// Submit creates a job in the queued state, kicks off work on a fresh
// goroutine, and returns the initial snapshot. The cancel handle is
// registered before the bus publish so a Cancel triggered by an
// immediate SSE consumer can't race in before the goroutine starts.
//
// The parent ctx is used only to plumb logger/values into the job (via
// context.WithoutCancel) — the job's lifetime is decoupled from the
// caller's so the work outlives its submitter (e.g. an HTTP request).
func (s *Scheduler) Submit(parent context.Context, invID inventory.ID, kind Kind, work Work) (Job, error) {
	id, err := newJobID()
	if err != nil {
		return Job{}, fmt.Errorf("mint job id: %w", err)
	}
	// Hold the lock long enough to (a) reject post-shutdown submits and
	// (b) register the cancel handle + bump the wait group atomically
	// with the goroutine launch. That way Shutdown's wg.Wait can't miss
	// a goroutine that was about to start.
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		cancel()

		return Job{}, ErrShutdown
	}
	// Dedup by inventory: one live job per inventory. The in-memory map is
	// authoritative — prior-process live jobs are marked aborted at boot,
	// so it always reflects exactly the jobs this process is running.
	if existing, ok := s.liveByInventory[invID]; ok {
		s.mu.Unlock()
		cancel()

		return Job{ID: existing, InventoryID: invID}, ErrDuplicateInventory
	}

	job := Job{
		ID:          id,
		InventoryID: invID,
		Kind:        kind,
		State:       StateQueued,
	}
	if err := s.store.Upsert(ctx, job); err != nil {
		s.mu.Unlock()
		cancel()

		return Job{}, fmt.Errorf("queue job %s: %w", job.ID, err)
	}
	s.cancels[job.ID] = cancel
	s.liveByInventory[invID] = job.ID
	s.wg.Add(1)
	s.mu.Unlock()

	s.bus.Publish(job)
	go s.run(ctx, cancel, job, work)

	return job, nil
}

// Cancel signals the cancel func associated with id. Returns ErrNotFound
// if the job isn't currently live (already finished, or never existed).
func (s *Scheduler) Cancel(id ID) error {
	s.mu.Lock()
	cancel, ok := s.cancels[id]
	s.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	cancel()

	return nil
}

// Shutdown cancels every live job, refuses new Submit calls
// (returning ErrShutdown), and waits for in-flight goroutines to
// finish, up to ctx's deadline. Idempotent. Call from the server's
// graceful shutdown path so in-flight builds don't outlive the
// process.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.shutdown = true
	for _, cancel := range s.cancels {
		cancel()
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown: %w", ctx.Err())
	}
}

func (s *Scheduler) run(ctx context.Context, cancel context.CancelFunc, job Job, work Work) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.cancels, job.ID)
		if s.liveByInventory[job.InventoryID] == job.ID {
			delete(s.liveByInventory, job.InventoryID)
		}
		s.mu.Unlock()
		cancel()
	}()

	// Wait for a concurrency slot. A job cancelled or shut down while
	// still queued never starts work — it transitions straight to
	// cancelled so the UI doesn't show it stuck in queued.
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		job.State = StateCancelled
		job.FinishedAt = time.Now()
		s.persistAndPublish(context.WithoutCancel(ctx), &job)

		return
	}

	job.State = StateRunning
	job.StartedAt = time.Now()
	s.persistAndPublish(ctx, &job)

	// report applies a non-zero diff to the job and broadcasts. A stage
	// transition resets quantitative progress so the UI doesn't show
	// stale done/total from the previous stage (e.g. "downloading 10/10"
	// while we've already moved on to "building").
	report := func(u Update) {
		if u.Stage != "" && u.Stage != job.Stage {
			job.Stage = u.Stage
			job.Progress = 0
			job.BytesDone = 0
			job.BytesTotal = 0
		}
		if u.Progress > 0 {
			job.Progress = u.Progress
		}
		if u.BytesTotal > 0 {
			job.BytesTotal = u.BytesTotal
		}
		if u.BytesDone > 0 {
			job.BytesDone = u.BytesDone
		}
		s.persistAndPublish(ctx, &job)
	}

	err := work(ctx, report)
	job.FinishedAt = time.Now()
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		job.State = StateCancelled
		if err != nil {
			job.Error = err.Error()
		}
	case err != nil:
		job.State = StateFailed
		job.Error = err.Error()
	default:
		job.State = StateSucceeded
		job.Progress = 100
	}
	// Final persist must succeed even after ctx is cancelled — that's
	// how the UI learns the job actually transitioned to cancelled/failed.
	s.persistAndPublish(context.WithoutCancel(ctx), &job)
}

func (s *Scheduler) persistAndPublish(ctx context.Context, j *Job) {
	if err := s.store.Upsert(ctx, *j); err != nil {
		// Storage failure shouldn't kill the worker, but the operator
		// needs to know. The job continues; subscribers see the
		// in-memory state via the bus.
		s.logger.Error().Err(err).Stringer("job_id", j.ID).Str("state", string(j.State)).
			Msg("persist job state")
	}
	s.bus.Publish(*j)
}

func newJobID() (ID, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("read random: %w", err)
	}

	return ID(hex.EncodeToString(b[:])), nil
}
