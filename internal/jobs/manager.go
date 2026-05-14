package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Work is the user's code for one job. It runs on a background
// goroutine. The report function posts progress updates back to
// subscribers. Returning an error transitions the job to failed;
// returning nil with no ctx cancellation transitions to succeeded.
type Work func(ctx context.Context, report func(Update)) error

// ErrNotFound is returned by Cancel for unknown job IDs.
var ErrNotFound = errors.New("job not found")

// Manager owns the in-memory registry of live jobs and their cancel
// handles. Job snapshots are persisted to Store on every transition and
// broadcast on Bus so the SSE handler can push updates to the UI.
type Manager struct {
	store *Store
	bus   *Bus

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

// NewManager wires a Store and a Bus.
func NewManager(store *Store, bus *Bus) *Manager {
	return &Manager{
		store:   store,
		bus:     bus,
		cancels: make(map[string]context.CancelFunc),
	}
}

// Submit creates a job in the queued state, kicks off work on a fresh
// goroutine, and returns the initial snapshot. The job ID is unique
// across the process lifetime.
func (m *Manager) Submit(invID string, kind Kind, work Work) (Job, error) {
	job := Job{
		ID:          newJobID(),
		InventoryID: invID,
		Kind:        kind,
		State:       StateQueued,
	}
	if err := m.store.Upsert(job); err != nil {
		return Job{}, fmt.Errorf("queue job %s: %w", job.ID, err)
	}
	m.bus.Publish(job)

	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancels[job.ID] = cancel
	m.mu.Unlock()

	go m.run(ctx, cancel, job, work)
	return job, nil
}

// Cancel signals the cancel func associated with id. Returns ErrNotFound
// if the job isn't currently live (already finished, or never existed).
func (m *Manager) Cancel(id string) error {
	m.mu.Lock()
	cancel, ok := m.cancels[id]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	cancel()
	return nil
}

func (m *Manager) run(ctx context.Context, cancel context.CancelFunc, job Job, work Work) {
	defer func() {
		m.mu.Lock()
		delete(m.cancels, job.ID)
		m.mu.Unlock()
		cancel()
	}()

	job.State = StateRunning
	job.StartedAt = time.Now()
	m.persistAndPublish(&job)

	// report applies a non-zero diff to the job and broadcasts.
	report := func(u Update) {
		if u.Stage != "" {
			job.Stage = u.Stage
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
		m.persistAndPublish(&job)
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
	m.persistAndPublish(&job)
}

func (m *Manager) persistAndPublish(j *Job) {
	if err := m.store.Upsert(*j); err != nil {
		// Storage failure shouldn't kill the worker — log via bus
		// subscriber on the application side. For now, swallow.
		_ = err
	}
	m.bus.Publish(*j)
}

func newJobID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
