package inventory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Config describes auto-load behaviour for a single inventory
// configuration (source bucket + inventory name). Persisted in the
// inventory_configs table so toggles survive restarts.
type Config struct {
	PollBackoffUntil time.Time `json:"poll_backoff_until,omitzero"`
	LastPolledAt     time.Time `json:"last_polled_at,omitzero"`
	Source           string    `json:"source"`
	Name             string    `json:"name"`
	LastPollError    string    `json:"last_poll_error,omitempty"`
	RetentionCount   uint32    `json:"retention_count"`
	PollFailureCount uint32    `json:"poll_failure_count,omitempty"`
	AutoLoad         bool      `json:"auto_load"`
}

// ConfigID is the natural key — "<source>/<name>" — for use in URLs
// and template lookups.
func (c Config) ConfigID() string { return c.Source + "/" + c.Name }

// DefaultRetentionCount is the per-config retention used when a Config
// row is created without an override and the store has no other
// default (WithDefaultRetention).
const DefaultRetentionCount uint32 = 2

// ConfigStore reads and writes inventory configurations.
type ConfigStore struct {
	db               *sql.DB
	defaultRetention uint32
}

// NewConfigStore returns a ConfigStore. The schema must already be
// migrated.
func NewConfigStore(db *sql.DB) *ConfigStore {
	return &ConfigStore{db: db, defaultRetention: DefaultRetentionCount}
}

// WithDefaultRetention sets the retention a row gets when it is written
// without one (the server's auto_load_retention_default); 0 keeps
// DefaultRetentionCount.
func (s *ConfigStore) WithDefaultRetention(n uint32) *ConfigStore {
	if n > 0 {
		s.defaultRetention = n
	}

	return s
}

// DefaultRetention is the retention a row written without one gets.
func (s *ConfigStore) DefaultRetention() uint32 { return s.defaultRetention }

const configSelectCols = `
    source, name, auto_load, retention_count,
    poll_failure_count, poll_backoff_until, last_polled_at, last_poll_error`

// Upsert writes the config row, replacing any existing row keyed by
// (source, name). Zero retention is rewritten to the store's default
// retention so callers can pass a zero-value Config and get sane
// defaults.
func (s *ConfigStore) Upsert(ctx context.Context, c Config) error {
	if c.RetentionCount == 0 {
		c.RetentionCount = s.defaultRetention
	}
	autoLoad := 0
	if c.AutoLoad {
		autoLoad = 1
	}
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO inventory_configs (
            source, name, auto_load, retention_count,
            poll_failure_count, poll_backoff_until, last_polled_at,
            last_poll_error, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, name) DO UPDATE SET
            auto_load          = excluded.auto_load,
            retention_count    = excluded.retention_count,
            poll_failure_count = excluded.poll_failure_count,
            poll_backoff_until = excluded.poll_backoff_until,
            last_polled_at     = excluded.last_polled_at,
            last_poll_error    = excluded.last_poll_error,
            updated_at         = excluded.updated_at`,
		c.Source, c.Name, autoLoad, c.RetentionCount,
		c.PollFailureCount, UnixOrZero(c.PollBackoffUntil), UnixOrZero(c.LastPolledAt),
		c.LastPollError, time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("upsert config %s/%s: %w", c.Source, c.Name, err)
	}

	return nil
}

// SetPolicy writes the user-controlled columns (auto_load, retention)
// and leaves the poller's columns alone, creating the row if needed.
// Zero retention means the store's default retention.
func (s *ConfigStore) SetPolicy(ctx context.Context, source, name string, autoLoad bool, retention uint32) error {
	if retention == 0 {
		retention = s.defaultRetention
	}
	flag := 0
	if autoLoad {
		flag = 1
	}
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO inventory_configs (source, name, auto_load, retention_count, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source, name) DO UPDATE SET
            auto_load       = excluded.auto_load,
            retention_count = excluded.retention_count,
            updated_at      = excluded.updated_at`,
		source, name, flag, retention, time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("set policy %s/%s: %w", source, name, err)
	}

	return nil
}

// PollState is the poller's bookkeeping for one configuration.
type PollState struct {
	LastPolledAt time.Time
	BackoffUntil time.Time
	LastError    string
	FailureCount uint32
}

// RecordPoll writes only the poller's columns, so a poll that ran while
// the user changed auto_load or retention never overwrites that change.
func (s *ConfigStore) RecordPoll(ctx context.Context, source, name string, p PollState) error {
	_, err := s.db.ExecContext(ctx, `
        UPDATE inventory_configs SET
            poll_failure_count = ?,
            poll_backoff_until = ?,
            last_polled_at     = ?,
            last_poll_error    = ?,
            updated_at         = ?
        WHERE source = ? AND name = ?`,
		p.FailureCount, UnixOrZero(p.BackoffUntil), UnixOrZero(p.LastPolledAt), p.LastError,
		time.Now().Unix(), source, name,
	)
	if err != nil {
		return fmt.Errorf("record poll %s/%s: %w", source, name, err)
	}

	return nil
}

// Get returns ErrStoreNotFound when no row matches.
func (s *ConfigStore) Get(ctx context.Context, source, name string) (Config, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+configSelectCols+` FROM inventory_configs WHERE source = ? AND name = ?`, source, name)
	c, err := scanConfig(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Config{}, ErrStoreNotFound
	}
	if err != nil {
		return Config{}, fmt.Errorf("get config %s/%s: %w", source, name, err)
	}

	return c, nil
}

// List returns every config row ordered by (source, name).
func (s *ConfigStore) List(ctx context.Context) ([]Config, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+configSelectCols+` FROM inventory_configs ORDER BY source, name`)
	if err != nil {
		return nil, fmt.Errorf("list configs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Config
	for rows.Next() {
		c, err := scanConfig(rows)
		if err != nil {
			return nil, fmt.Errorf("scan config row: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate configs: %w", err)
	}

	return out, nil
}

func scanConfig(r rowScanner) (Config, error) {
	var c Config
	var autoLoad int
	var backoff, polled int64
	if err := r.Scan(
		&c.Source, &c.Name, &autoLoad, &c.RetentionCount,
		&c.PollFailureCount, &backoff, &polled, &c.LastPollError,
	); err != nil {
		return Config{}, fmt.Errorf("scan config: %w", err)
	}
	c.AutoLoad = autoLoad != 0
	c.PollBackoffUntil = TimeFromUnix(backoff)
	c.LastPolledAt = TimeFromUnix(polled)

	return c, nil
}
