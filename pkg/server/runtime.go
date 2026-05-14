package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/internal/migrate"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// RuntimeOptions is the binary-level configuration the server entry
// point parses from flags/env. Separated from Config so the orchestration
// of "load price table, open state db, build Config, run" can live in
// one place that's testable independently of the main goroutine.
type RuntimeOptions struct {
	Addr     string
	S3Source string
	CacheDir string

	// StateDB is the SQLite path for persisted inventory + job state.
	// Empty falls back to <CacheDir>/state.db; when CacheDir is also
	// empty SQLite opens in-memory and state is lost on restart.
	StateDB string

	// PriceTablePath, when empty, falls back to
	// pricing.DefaultUSEast1Prices.
	PriceTablePath string

	Logger zerolog.Logger
}

// Bootstrap turns RuntimeOptions into a ready-to-Run server: it
// resolves the state DB path, opens the SQLite connection, loads the
// price table, and constructs Config + Server. The returned cleanup
// closes the DB; callers must invoke it on exit.
//
// The error path closes any partially-initialised resources so the
// caller never leaks a half-open handle on failure.
func Bootstrap(opts RuntimeOptions) (srv *Server, cleanup func(), err error) {
	priceTable, err := loadPriceTable(opts.PriceTablePath, opts.Logger)
	if err != nil {
		return nil, nil, err
	}

	dbPath := resolveStateDBPath(opts.StateDB, opts.CacheDir)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, nil, fmt.Errorf("ensure state-db parent dir: %w", err)
	}
	db, err := OpenStateDB(dbPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open state db: %w", err)
	}
	opts.Logger.Info().Str("path", dbPath).Msg("opened state db")

	cleanup = func() {
		if err := db.Close(); err != nil {
			opts.Logger.Error().Err(err).Msg("close state db")
		}
	}

	if err := migrate.Apply(db); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("apply migrations: %w", err)
	}
	version, dirty, err := migrate.Version(db)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("read schema version: %w", err)
	}
	opts.Logger.Info().Uint("schema_version", version).Bool("dirty", dirty).Msg("schema migrated")

	srv, err = New(Config{
		Addr:       opts.Addr,
		Logger:     opts.Logger,
		PriceTable: priceTable,
		S3Source:   opts.S3Source,
		CacheDir:   opts.CacheDir,
		DB:         db,
	})
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("create server: %w", err)
	}
	return srv, cleanup, nil
}

// BootstrapAndRun is the full happy-path of the binary, factored out so
// the main goroutine is just signal handling and exit-code reporting.
// The S3 client probe inside Bootstrap uses its own bounded context
// (see newDiscoveryWiring) so callers don't need to budget for it.
//
//nolint:contextcheck // Bootstrap mints a fresh, bounded ctx for the S3 client probe
func BootstrapAndRun(ctx context.Context, opts RuntimeOptions) error {
	srv, cleanup, err := Bootstrap(opts)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := srv.Run(ctx); err != nil {
		return fmt.Errorf("run server: %w", err)
	}
	return nil
}

func loadPriceTable(path string, logger zerolog.Logger) (pricing.PriceTable, error) {
	if path == "" {
		return pricing.DefaultUSEast1Prices(), nil
	}
	pt, err := pricing.LoadPriceTable(path)
	if err != nil {
		return pricing.PriceTable{}, fmt.Errorf("load price table %s: %w", path, err)
	}
	logger.Info().Str("path", path).Msg("loaded custom price table")
	return pt, nil
}

// resolveStateDBPath defaults the SQLite path to <cacheDir>/state.db
// when StateDB is empty. Returning the resolved path makes the choice
// inspectable in tests + logs.
func resolveStateDBPath(stateDB, cacheDir string) string {
	if stateDB != "" {
		return stateDB
	}
	if cacheDir != "" {
		return filepath.Join(cacheDir, "state.db")
	}
	return ""
}
