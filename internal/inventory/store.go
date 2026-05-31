package inventory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Store persists Info records to SQLite so the in-memory Manager can be
// rehydrated on server restart. The Manager remains the runtime cache —
// Store is just durable backing.
type Store struct {
	db *sql.DB
}

// NewStore returns a Store backed by db. The schema must be migrated
// (see internal/migrate) before NewStore is called. The DB must be
// opened with PRAGMA foreign_keys=1 so the jobs FK cascade fires on
// Delete.
func NewStore(db *sql.DB) (*Store, error) {
	return &Store{db: db}, nil
}

// Upsert writes info, replacing any existing row with the same ID.
// UpdatedAt is set to time.Now().
func (s *Store) Upsert(ctx context.Context, info Info) error {
	hasTier := 0
	if info.HasTierData {
		hasTier = 1
	}
	pinned := 0
	if info.Pinned {
		pinned = 1
	}
	loadedAt := UnixOrZero(info.LoadedAt)
	userUnloadedAt := UnixOrZero(info.UserUnloadedAt)
	backoffUntil := UnixOrZero(info.AutoLoadBackoffUntil)
	failedAt := UnixOrZero(info.LastAutoLoadFailedAt)
	lastAccessed := UnixOrZero(info.LastAccessedAt)
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO inventories (
            id, name, path, state, error,
            node_count, max_depth, has_tier_data,
            loaded_at, pinned, user_unloaded_at, index_bytes,
            auto_load_failure_count, auto_load_backoff_until,
            last_auto_load_failed_at,
            last_accessed_at, load_duration_ns, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name                     = excluded.name,
            path                     = excluded.path,
            state                    = excluded.state,
            error                    = excluded.error,
            node_count               = excluded.node_count,
            max_depth                = excluded.max_depth,
            has_tier_data            = excluded.has_tier_data,
            loaded_at                = excluded.loaded_at,
            pinned                   = excluded.pinned,
            user_unloaded_at         = excluded.user_unloaded_at,
            index_bytes              = excluded.index_bytes,
            auto_load_failure_count  = excluded.auto_load_failure_count,
            auto_load_backoff_until  = excluded.auto_load_backoff_until,
            last_auto_load_failed_at = excluded.last_auto_load_failed_at,
            last_accessed_at         = excluded.last_accessed_at,
            load_duration_ns         = excluded.load_duration_ns,
            updated_at               = excluded.updated_at`,
		info.ID, info.Name, info.Path, string(info.State), info.Error,
		info.NodeCount, info.MaxDepth, hasTier,
		loadedAt, pinned, userUnloadedAt, info.IndexBytes,
		info.AutoLoadFailureCount, backoffUntil, failedAt,
		lastAccessed, int64(info.LoadDuration), time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("upsert inventory %s: %w", info.ID, err)
	}

	return nil
}

const inventorySelectCols = `
    id, name, path, state, error,
    node_count, max_depth, has_tier_data, loaded_at,
    pinned, user_unloaded_at, index_bytes,
    auto_load_failure_count, auto_load_backoff_until,
    last_auto_load_failed_at,
    last_accessed_at, load_duration_ns`

// Get returns ErrStoreNotFound when no row matches.
func (s *Store) Get(ctx context.Context, id ID) (Info, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+inventorySelectCols+` FROM inventories WHERE id = ?`, id)
	info, err := scanInfo(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Info{}, ErrStoreNotFound
	}
	if err != nil {
		return Info{}, fmt.Errorf("get inventory %s: %w", id, err)
	}

	return info, nil
}

// List returns every persisted inventory ordered by id.
func (s *Store) List(ctx context.Context) ([]Info, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+inventorySelectCols+` FROM inventories ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list inventories: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Info
	for rows.Next() {
		info, err := scanInfo(rows)
		if err != nil {
			return nil, fmt.Errorf("scan inventory row: %w", err)
		}
		out = append(out, info)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inventories: %w", err)
	}

	return out, nil
}

// Delete removes one inventory by ID. ON DELETE CASCADE on the jobs
// table wipes related job rows too.
func (s *Store) Delete(ctx context.Context, id ID) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM inventories WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete inventory %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return ErrStoreNotFound
	}

	return nil
}

// ErrStoreNotFound is the Store-level "row missing" sentinel. Kept
// distinct from ErrNotFound (the Manager's runtime "id unknown" error)
// so callers can tell durable absence from in-memory absence.
var ErrStoreNotFound = errors.New("inventory not in store")

func scanInfo(r rowScanner) (Info, error) {
	var info Info
	var state string
	var hasTier, pinned int
	var loadedAt, userUnloadedAt, backoffUntil, failedAt, lastAccessed, loadDurationNs int64
	if err := r.Scan(
		&info.ID, &info.Name, &info.Path, &state, &info.Error,
		&info.NodeCount, &info.MaxDepth, &hasTier, &loadedAt,
		&pinned, &userUnloadedAt, &info.IndexBytes,
		&info.AutoLoadFailureCount, &backoffUntil, &failedAt,
		&lastAccessed, &loadDurationNs,
	); err != nil {
		return Info{}, fmt.Errorf("scan inventory: %w", err)
	}
	info.State = State(state)
	info.HasTierData = hasTier != 0
	info.Pinned = pinned != 0
	info.LoadedAt = TimeFromUnix(loadedAt)
	info.UserUnloadedAt = TimeFromUnix(userUnloadedAt)
	info.AutoLoadBackoffUntil = TimeFromUnix(backoffUntil)
	info.LastAutoLoadFailedAt = TimeFromUnix(failedAt)
	info.LastAccessedAt = TimeFromUnix(lastAccessed)
	info.LoadDuration = time.Duration(loadDurationNs)

	return info, nil
}

// UnixOrZero converts t to a Unix-seconds int64, returning 0 for the
// zero time so SQLite NULL-vs-epoch round-trips stay symmetric.
func UnixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	return t.Unix()
}

// TimeFromUnix converts a Unix-seconds int64 back to a time.Time,
// returning the zero time for 0 so it round-trips with UnixOrZero.
func TimeFromUnix(sec int64) time.Time {
	if sec == 0 {
		return time.Time{}
	}

	return time.Unix(sec, 0)
}

type rowScanner interface {
	Scan(dest ...any) error
}
