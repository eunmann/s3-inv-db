package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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
	StateDB  string // empty → <CacheDir>/state.db

	PriceTablePath string // empty → DefaultUSEast1Prices

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
//
//nolint:contextcheck // Bootstrap intentionally uses a fresh ctx for the S3 startup probes
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
