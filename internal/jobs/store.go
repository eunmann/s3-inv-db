package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// likeEscape escapes the SQL LIKE metacharacters in s so it matches
// literally. Pair with `ESCAPE '\'` in the query. Backslash is escaped
// first so it doesn't double-escape the inserted escape chars.
func likeEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)

	return s
}

// Store persists job snapshots so the UI can show jobs across restarts.
type Store struct {
	db *sql.DB
}

// ErrStoreNotFound is returned for missing rows.
var ErrStoreNotFound = errors.New("job not in store")

// NewStore returns a Store backed by db. The schema must already be
// migrated (see internal/migrate).
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// jobColumns is the shared column list for every Job CRUD statement.
// The bytes_total / bytes_done columns store StageTotal / StageDone —
// the names predate progress units becoming polymorphic (chunks for
// download, run files for build, …). Kept to avoid a schema migration.
const jobColumns = `id, inventory_id, kind, state, stage, progress,
               bytes_total, bytes_done, started_at, finished_at,
               error, updated_at, stages_json, attempt_count, prev_job_id,
               spill_count, spill_bytes, merge_rounds, merge_bytes,
               step, step_started_at`

// Upsert writes j by primary key. UpdatedAt is set to time.Now().
func (s *Store) Upsert(ctx context.Context, j Job) error {
	stagesJSON, err := marshalStages(j.Stages)
	if err != nil {
		return fmt.Errorf("marshal job %s stages: %w", j.ID, err)
	}
	attempt := j.AttemptCount
	if attempt <= 0 {
		attempt = 1
	}
	_, err = s.db.ExecContext(ctx, `
        INSERT INTO jobs (`+jobColumns+`)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            state         = excluded.state,
            stage         = excluded.stage,
            progress      = excluded.progress,
            bytes_total   = excluded.bytes_total,
            bytes_done    = excluded.bytes_done,
            started_at    = excluded.started_at,
            finished_at   = excluded.finished_at,
            error         = excluded.error,
            updated_at    = excluded.updated_at,
            stages_json   = excluded.stages_json,
            attempt_count = excluded.attempt_count,
            prev_job_id   = excluded.prev_job_id,
            spill_count   = excluded.spill_count,
            spill_bytes   = excluded.spill_bytes,
            merge_rounds  = excluded.merge_rounds,
            merge_bytes   = excluded.merge_bytes,
            step          = excluded.step,
            step_started_at = excluded.step_started_at`,
		j.ID, j.InventoryID, string(j.Kind), string(j.State), j.Stage,
		j.Progress, j.StageTotal, j.StageDone,
		inventory.UnixOrZero(j.StartedAt), inventory.UnixOrZero(j.FinishedAt),
		j.Error, time.Now().Unix(),
		stagesJSON, attempt, string(j.PrevJobID),
		j.SpillCount, j.SpillBytes, j.MergeRounds, j.MergeBytes,
		j.Step, inventory.UnixOrZero(j.StepStartedAt),
	)
	if err != nil {
		return fmt.Errorf("upsert job %s: %w", j.ID, err)
	}

	return nil
}

// Get fetches one job by id.
func (s *Store) Get(ctx context.Context, id ID) (Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = ?`, id)
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
func (s *Store) ListForInventory(ctx context.Context, invID inventory.ID) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE inventory_id = ? ORDER BY updated_at DESC`, invID)
	if err != nil {
		return nil, fmt.Errorf("list jobs for %s: %w", invID, err)
	}
	defer func() { _ = rows.Close() }()

	return scanJobs(rows)
}

// latestOrder orders an inventory's jobs newest first; rowid breaks
// updated_at ties, so LatestForInventory and LatestByInventory agree.
const latestOrder = `updated_at DESC, rowid DESC`

// LatestForInventory returns the most recently updated job for an
// inventory, or ErrStoreNotFound if none exist.
func (s *Store) LatestForInventory(ctx context.Context, invID inventory.ID) (Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE inventory_id = ? ORDER BY `+latestOrder+` LIMIT 1`, invID)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrStoreNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("latest job for %s: %w", invID, err)
	}

	return j, nil
}

// LatestByInventory returns every inventory's most recently updated job
// in one query: what LatestForInventory returns, for all of them.
func (s *Store) LatestByInventory(ctx context.Context) (map[inventory.ID]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM (
	    SELECT `+jobColumns+`, ROW_NUMBER() OVER (PARTITION BY inventory_id ORDER BY `+latestOrder+`) AS rn FROM jobs
	) WHERE rn = 1`)
	if err != nil {
		return nil, fmt.Errorf("latest jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	js, err := scanJobs(rows)
	if err != nil {
		return nil, fmt.Errorf("latest jobs: %w", err)
	}
	out := make(map[inventory.ID]Job, len(js))
	for i := range js {
		out[js[i].InventoryID] = js[i]
	}

	return out, nil
}

// LatestSuccessfulBuildForConfig returns the newest succeeded build job
// for any run of a given inventory configuration ("<src>/<name>"),
// excluding excludeID so the caller's own just-succeeded job can't be
// picked as its own baseline. Used by the drawer to project an ETA from
// a prior baseline. Empty configID returns ErrStoreNotFound rather than
// matching every row.
func (s *Store) LatestSuccessfulBuildForConfig(ctx context.Context, configID string, excludeID ID) (Job, error) {
	if configID == "" {
		return Job{}, ErrStoreNotFound
	}
	// LIKE-escape configID so '_'/'%' in inventory names match literally
	// rather than as wildcards, then append "/%" to match any run under
	// the configuration. The escape char is '\' (declared via ESCAPE).
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+`
          FROM jobs
         WHERE inventory_id LIKE ? ESCAPE '\' AND id != ?
              AND state = 'succeeded' AND kind = 'build'
              AND started_at > 0 AND finished_at > 0
         ORDER BY finished_at DESC LIMIT 1`, likeEscape(configID)+"/%", string(excludeID))
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrStoreNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("baseline build for %s: %w", configID, err)
	}

	return j, nil
}

// MarkAborted moves every job in any of the given states to "aborted"
// and stamps reason in the error column. Used at server boot to clear
// orphaned in-flight jobs.
func (s *Store) MarkAborted(ctx context.Context, reason string, fromStates ...State) (int64, error) {
	if len(fromStates) == 0 {
		return 0, nil
	}
	const stmt = `UPDATE jobs SET state = 'aborted', error = ?, finished_at = ?, updated_at = ?
          WHERE state = ?`
	now := time.Now().Unix()
	var total int64
	for _, st := range fromStates {
		res, err := s.db.ExecContext(ctx, stmt, reason, now, now, string(st))
		if err != nil {
			return total, fmt.Errorf("mark aborted (%s): %w", st, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("rows affected: %w", err)
		}
		total += n
	}

	return total, nil
}

// Job-history retention: terminal jobs last updated more than
// RetentionWindow ago are pruned, except that each inventory always
// keeps its RetentionKeepPerInventory most recent jobs so the drawer
// and retry chain still have history for rarely-rebuilt runs.
const (
	RetentionWindow           = 30 * 24 * time.Hour
	RetentionKeepPerInventory = 20
)

// PruneTerminal deletes terminal jobs last updated before cutoff that
// fall outside their inventory's keep most recent jobs. Queued and
// running jobs are never deleted. Returns the number of rows removed.
func (s *Store) PruneTerminal(ctx context.Context, cutoff time.Time, keep int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
        DELETE FROM jobs WHERE id IN (
            SELECT id FROM (
                SELECT id, state, updated_at,
                       ROW_NUMBER() OVER (PARTITION BY inventory_id ORDER BY updated_at DESC, id DESC) AS rank
                  FROM jobs
            )
             WHERE rank > ? AND updated_at < ?
               AND state IN (?, ?, ?, ?)
        )`,
		keep, cutoff.Unix(),
		string(StateSucceeded), string(StateFailed), string(StateCancelled), string(StateAborted),
	)
	if err != nil {
		return 0, fmt.Errorf("prune terminal jobs: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune terminal jobs: rows affected: %w", err)
	}

	return n, nil
}

func scanJob(r rowScanner) (Job, error) {
	var j Job
	var kind, state, stagesJSON, prevJobID string
	var startedAt, finishedAt, updatedAt, stepStartedAt int64
	var attemptCount int
	if err := r.Scan(
		&j.ID, &j.InventoryID, &kind, &state, &j.Stage, &j.Progress,
		&j.StageTotal, &j.StageDone, &startedAt, &finishedAt, &j.Error, &updatedAt,
		&stagesJSON, &attemptCount, &prevJobID,
		&j.SpillCount, &j.SpillBytes, &j.MergeRounds, &j.MergeBytes,
		&j.Step, &stepStartedAt,
	); err != nil {
		return Job{}, fmt.Errorf("scan job: %w", err)
	}
	j.Kind = Kind(kind)
	j.State = State(state)
	j.StartedAt = inventory.TimeFromUnix(startedAt)
	j.FinishedAt = inventory.TimeFromUnix(finishedAt)
	j.StepStartedAt = inventory.TimeFromUnix(stepStartedAt)
	j.UpdatedAt = time.Unix(updatedAt, 0)
	stages, err := unmarshalStages(stagesJSON)
	if err != nil {
		return Job{}, fmt.Errorf("scan job %s stages: %w", j.ID, err)
	}
	j.Stages = stages
	j.AttemptCount = attemptCount
	j.PrevJobID = ID(prevJobID)

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

// marshalStages always emits a non-null payload to satisfy the
// stages_json NOT NULL DEFAULT '[]' invariant.
func marshalStages(s []StageRecord) (string, error) {
	if len(s) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("marshal stages: %w", err)
	}

	return string(b), nil
}

func unmarshalStages(raw string) ([]StageRecord, error) {
	if raw == "" || raw == "[]" {
		return nil, nil
	}
	var out []StageRecord
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("unmarshal stages %q: %w", raw, err)
	}

	return out, nil
}
