// Package migrate applies the embedded SQLite schema migrations.
package migrate

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	sqlitedrv "github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Apply runs every pending up-migration against db. Idempotent.
func Apply(db *sql.DB) error {
	m, err := newMigrator(db)
	if err != nil {
		return err
	}
	if err := recoverDirty(m); err != nil {
		return err
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// recoverDirty clears the dirty flag if set. Safe because every up
// migration is idempotent.
func recoverDirty(m *migrate.Migrate) error {
	version, dirty, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if !dirty {
		return nil
	}
	if err := m.Force(int(version)); err != nil {
		return fmt.Errorf("clear dirty version %d: %w", version, err)
	}
	return nil
}

// Down rolls back the most recent steps migrations.
func Down(db *sql.DB, steps int) error {
	m, err := newMigrator(db)
	if err != nil {
		return err
	}
	if err := m.Steps(-steps); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("roll back %d migrations: %w", steps, err)
	}
	return nil
}

// Version reports the current schema version and whether the DB is in
// a dirty (interrupted) migration state. Returns (0, false, nil) for
// a brand-new DB with no migrations recorded.
func Version(db *sql.DB) (version uint, dirty bool, err error) {
	m, mErr := newMigrator(db)
	if mErr != nil {
		return 0, false, mErr
	}
	version, dirty, err = m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read schema version: %w", err)
	}
	return version, dirty, nil
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
