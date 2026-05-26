// Package migrate applies the embedded SQLite schema migrations.
package migrate

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	sqlitedrv "github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Apply runs every pending up-migration against db. Idempotent.
//
// Dirty-state recovery: when schema_migrations says (version=N,
// dirty=true) the prior process aborted while applying N. The naive
// fix — Force(N) — clears the dirty flag without re-running the SQL,
// so a partial migration's effects are silently stranded. We rewind
// to N-1 and let Up() retry from scratch.
//
// The retry can fail with an "already exists" error in the rare race
// where N's SQL committed but the process died before clearing dirty.
// AlreadyAppliedErr below treats that as success: the schema is
// already at N, we just force-advance and continue.
func Apply(db *sql.DB) error {
	m, err := newMigrator(db)
	if err != nil {
		return err
	}
	version, dirty, vErr := m.Version()
	switch {
	case errors.Is(vErr, migrate.ErrNilVersion):
		// fresh DB; nothing to recover
	case vErr != nil:
		return fmt.Errorf("read schema version: %w", vErr)
	case dirty:
		if err := rewindFromDirty(m, version); err != nil {
			return err
		}
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		if dirty && alreadyAppliedErr(err) {
			// N's effects were already in the DB; force-advance and
			// re-Up so any later pending migrations still run.
			if ferr := m.Force(int(version)); ferr != nil {
				return fmt.Errorf("force-advance after already-applied retry: %w", ferr)
			}
			if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
				return fmt.Errorf("apply migrations after force-advance: %w", err)
			}

			return nil
		}

		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}

// rewindFromDirty resets schema_migrations to the last cleanly-applied
// version (current - 1) so the next Up re-runs the migration that
// failed. Golang-migrate has no "force to previous version" primitive;
// the established workaround is Force(N-1).
func rewindFromDirty(m *migrate.Migrate, current uint) error {
	if current == 0 {
		// Dirty at version 0 means the very first migration aborted
		// mid-way. Force(-1) is the documented "nil" sentinel that
		// rewinds to the pre-migration state.
		if err := m.Force(-1); err != nil {
			return fmt.Errorf("rewind from dirty v0: %w", err)
		}

		return nil
	}
	if err := m.Force(int(current) - 1); err != nil {
		return fmt.Errorf("rewind from dirty v%d to v%d: %w", current, current-1, err)
	}

	return nil
}

// alreadyAppliedErr reports whether err looks like the SQL driver
// complaining that the migration's effects already exist (duplicate
// column on ADD COLUMN, table already exists, etc.). String-matching
// is a deliberate choice — the modernc.org/sqlite driver's errors
// don't expose a typed sentinel, and the alternative (introspecting
// the schema before every migration) would couple this package to
// each migration's contents.
func alreadyAppliedErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"duplicate column",
		"already exists",
		"already in",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}

	return false
}

// VersionInfo describes the migration state of the schema_migrations
// table: the major version number and whether the previous migration
// finished cleanly.
type VersionInfo struct {
	Version uint
	Dirty   bool
}

// Version reports the current schema version and whether the DB is in
// a dirty (interrupted) migration state. Returns the zero VersionInfo
// for a brand-new DB with no migrations recorded.
func Version(db *sql.DB) (VersionInfo, error) {
	m, err := newMigrator(db)
	if err != nil {
		return VersionInfo{}, err
	}
	version, dirty, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return VersionInfo{}, nil
	}
	if err != nil {
		return VersionInfo{}, fmt.Errorf("read schema version: %w", err)
	}

	return VersionInfo{Version: version, Dirty: dirty}, nil
}

// newMigrator builds a Migrate bound to db. Do not call m.Close() —
// the WithInstance sqlite driver's Close closes db. Let the Migrate
// value be garbage-collected instead.
func newMigrator(db *sql.DB) (*migrate.Migrate, error) {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("open embedded migrations: %w", err)
	}
	driver, err := sqlitedrv.WithInstance(db, &sqlitedrv.Config{})
	if err != nil {
		return nil, fmt.Errorf("wrap sqlite driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "sqlite", driver)
	if err != nil {
		return nil, fmt.Errorf("build migrator: %w", err)
	}

	return m, nil
}
