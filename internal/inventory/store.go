package inventory

import (
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

// NewStore returns a Store backed by db. The schema must already be
// migrated (server.Bootstrap calls migrate.Apply before constructing
// stores; tests use migrate.Apply from internal/migrate or testmigrate).
// The db must be opened with PRAGMA foreign_keys=1 so the jobs.jobs FK
// cascade fires when an inventory is deleted.
func NewStore(db *sql.DB) (*Store, error) {
	return &Store{db: db}, nil
}

// Upsert writes info, replacing any existing row with the same ID.
// UpdatedAt is set to time.Now().
func (s *Store) Upsert(info Info) error {
	hasTier := 0
	if info.HasTierData {
		hasTier = 1
	}
	loadedAt := int64(0)
	if !info.LoadedAt.IsZero() {
		loadedAt = info.LoadedAt.Unix()
	}
	_, err := s.db.Exec(`
        INSERT INTO inventories (
            id, name, path, state, error,
            node_count, max_depth, has_tier_data,
            loaded_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name          = excluded.name,
            path          = excluded.path,
            state         = excluded.state,
            error         = excluded.error,
            node_count    = excluded.node_count,
            max_depth     = excluded.max_depth,
            has_tier_data = excluded.has_tier_data,
            loaded_at     = excluded.loaded_at,
            updated_at    = excluded.updated_at`,
		info.ID, info.Name, info.Path, string(info.State), info.Error,
		info.NodeCount, info.MaxDepth, hasTier,
		loadedAt, time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("upsert inventory %s: %w", info.ID, err)
	}
	return nil
}

// Get fetches one inventory by ID. Returns ErrStoreNotFound when the
// row is missing.
func (s *Store) Get(id ID) (Info, error) {
	row := s.db.QueryRow(`
        SELECT id, name, path, state, error,
               node_count, max_depth, has_tier_data, loaded_at
          FROM inventories WHERE id = ?`, id)
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
func (s *Store) List() ([]Info, error) {
	rows, err := s.db.Query(`
        SELECT id, name, path, state, error,
               node_count, max_depth, has_tier_data, loaded_at
          FROM inventories ORDER BY id`)
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
func (s *Store) Delete(id ID) error {
	res, err := s.db.Exec(`DELETE FROM inventories WHERE id = ?`, id)
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
	var hasTier int
	var loadedAt int64
	if err := r.Scan(
		&info.ID, &info.Name, &info.Path, &state, &info.Error,
		&info.NodeCount, &info.MaxDepth, &hasTier, &loadedAt,
	); err != nil {
		return Info{}, fmt.Errorf("scan inventory: %w", err)
	}
	info.State = State(state)
	info.HasTierData = hasTier != 0
	if loadedAt != 0 {
		info.LoadedAt = time.Unix(loadedAt, 0)
	}
	return info, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}
