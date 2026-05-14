package jobs

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// Store persists job snapshots so the UI can show jobs across restarts.
type Store struct {
	db *sql.DB
}

// ErrStoreNotFound is returned for missing rows.
var ErrStoreNotFound = errors.New("job not in store")

// NewStore returns a Store backed by db. The schema must already be
// migrated (see internal/migrate).
func NewStore(db *sql.DB) (*Store, error) {
	return &Store{db: db}, nil
}

// Upsert writes j by primary key. UpdatedAt is set to time.Now().
func (s *Store) Upsert(j Job) error {
	_, err := s.db.Exec(`
        INSERT INTO jobs (
            id, inventory_id, kind, state, stage, progress,
            bytes_total, bytes_done, started_at, finished_at,
            error, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            state       = excluded.state,
            stage       = excluded.stage,
            progress    = excluded.progress,
            bytes_total = excluded.bytes_total,
            bytes_done  = excluded.bytes_done,
            started_at  = excluded.started_at,
            finished_at = excluded.finished_at,
            error       = excluded.error,
            updated_at  = excluded.updated_at`,
		j.ID, j.InventoryID, string(j.Kind), string(j.State), j.Stage,
		j.Progress, j.BytesTotal, j.BytesDone,
		unixOrZero(j.StartedAt), unixOrZero(j.FinishedAt),
		j.Error, time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("upsert job %s: %w", j.ID, err)
	}
	return nil
}

// Get fetches one job by id.
func (s *Store) Get(id ID) (Job, error) {
	row := s.db.QueryRow(`
        SELECT id, inventory_id, kind, state, stage, progress,
               bytes_total, bytes_done, started_at, finished_at,
               error, updated_at
          FROM jobs WHERE id = ?`, id)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrStoreNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job %s: %w", id, err)
	}
	return j, nil
}

// ListForInventory returns jobs for one inventory, newest first.
func (s *Store) ListForInventory(invID inventory.ID) ([]Job, error) {
	rows, err := s.db.Query(`
        SELECT id, inventory_id, kind, state, stage, progress,
               bytes_total, bytes_done, started_at, finished_at,
               error, updated_at
          FROM jobs WHERE inventory_id = ? ORDER BY updated_at DESC`, invID)
	if err != nil {
		return nil, fmt.Errorf("list jobs for %s: %w", invID, err)
	}
	defer func() { _ = rows.Close() }()
	return scanJobs(rows)
}

// LatestForInventory returns the most recently updated job for an
// inventory, or ErrStoreNotFound if none exist.
func (s *Store) LatestForInventory(invID inventory.ID) (Job, error) {
	row := s.db.QueryRow(`
        SELECT id, inventory_id, kind, state, stage, progress,
               bytes_total, bytes_done, started_at, finished_at,
               error, updated_at
          FROM jobs WHERE inventory_id = ? ORDER BY updated_at DESC LIMIT 1`, invID)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrStoreNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("latest job for %s: %w", invID, err)
	}
	return j, nil
}

// MarkAborted moves every job in any of the given states to "aborted"
// and stamps reason in the error column. Used at server boot to clear
// orphaned in-flight jobs.
func (s *Store) MarkAborted(reason string, fromStates ...State) (int64, error) {
	if len(fromStates) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(fromStates))
	args := []any{reason, time.Now().Unix(), time.Now().Unix()}
	for i, st := range fromStates {
		placeholders[i] = "?"
		args = append(args, string(st))
	}
	res, err := s.db.Exec(`
        UPDATE jobs SET state = 'aborted', error = ?, finished_at = ?, updated_at = ?
          WHERE state IN (`+strings.Join(placeholders, ", ")+`)`, args...)
	if err != nil {
		return 0, fmt.Errorf("mark aborted: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return n, nil
}

func scanJob(r rowScanner) (Job, error) {
	var j Job
	var kind, state string
	var startedAt, finishedAt, updatedAt int64
	if err := r.Scan(
		&j.ID, &j.InventoryID, &kind, &state, &j.Stage, &j.Progress,
		&j.BytesTotal, &j.BytesDone, &startedAt, &finishedAt, &j.Error, &updatedAt,
	); err != nil {
		return Job{}, fmt.Errorf("scan job: %w", err)
	}
	j.Kind = Kind(kind)
	j.State = State(state)
	if startedAt != 0 {
		j.StartedAt = time.Unix(startedAt, 0)
	}
	if finishedAt != 0 {
		j.FinishedAt = time.Unix(finishedAt, 0)
	}
	j.UpdatedAt = time.Unix(updatedAt, 0)
	return j, nil
}

func scanJobs(rows *sql.Rows) ([]Job, error) {
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}
	return out, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}
