package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/migrate"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// ErrAutoLoadWithoutBudget is returned by Bootstrap when AutoLoad is
// enabled without --max-index-disk. Forcing operators to size the
// budget up front prevents an unbounded poller from filling the disk.
var ErrAutoLoadWithoutBudget = errors.New("--auto-load requires --max-index-disk")

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

	// AutoLoad turns on the background poller that automatically loads
	// the newest run of every opted-in inventory configuration. When
	// true, MaxIndexDisk MUST be set — otherwise Bootstrap refuses to
	// start so the operator confronts the disk-budget decision before
	// the process discovers a billion-object inventory.
	AutoLoad bool

	// PollInterval governs how often the auto-loader hits S3 for new
	// runs. Default 15m.
	PollInterval time.Duration

	// MaxIndexDisk caps the on-disk index footprint in bytes. Required
	// when AutoLoad is true; refused otherwise to keep the configuration
	// fail-fast and explicit.
	MaxIndexDisk uint64

	// IndexHeadroomBytes is reserved unused space inside MaxIndexDisk so
	// a load that exceeds its estimate has room to grow. Default 20% of
	// MaxIndexDisk when MaxIndexDisk is set.
	IndexHeadroomBytes uint64

	// AutoLoadConcurrency caps in-flight auto-loads. Default 1.
	AutoLoadConcurrency int

	// AutoLoadRetentionDefault is the per-config retention used when
	// inventory_configs.retention_count is unset. Default 2.
	AutoLoadRetentionDefault uint32

	// IndexRatio is the multiplier applied to a manifest's total
	// compressed size to estimate the final index bytes. Default
	// inventory.DefaultIndexRatio.
	IndexRatio float64

	// DiscoveryRefreshInterval governs how often the background
	// discovery refresher updates the cached snapshot HTTP handlers
	// serve. Zero falls back to the server's default (1 minute).
	DiscoveryRefreshInterval time.Duration

	// QueryBatchMax caps the prefix count accepted by the batch stats
	// endpoint. Zero means use the handler default.
	QueryBatchMax int

	// MetricsAddr, when non-empty, binds /metrics on its own listener
	// (e.g. ":9090") so operators can keep it off the public router.
	MetricsAddr string

	// AutoLoadDryRun replaces side-effecting autoload actions with log
	// entries — operators can preview a policy change before flipping it.
	AutoLoadDryRun bool

	// InventoryConfigs declares per-configuration auto-load + retention
	// settings to upsert at startup. Typically populated from the JSON
	// config file.
	InventoryConfigs []InventoryConfigEntry

	Logger zerolog.Logger
}

// InventoryConfigEntry mirrors the JSON config file's inventories[].
// Duplicated here so pkg/server doesn't import internal/appconfig.
type InventoryConfigEntry struct {
	Source         string
	Name           string
	AutoLoad       bool
	RetentionCount uint32
}

// Bootstrap turns RuntimeOptions into a ready-to-Run server: it
// resolves the state DB path, opens the SQLite connection, loads the
// price table, and constructs Config + Server. The returned cleanup
// closes the DB; callers must invoke it on exit.
//
// The error path closes any partially-initialised resources so the
// caller never leaks a half-open handle on failure.
func Bootstrap(ctx context.Context, opts RuntimeOptions) (*Server, func(), error) {
	if opts.AutoLoad && opts.MaxIndexDisk == 0 {
		return nil, nil, ErrAutoLoadWithoutBudget
	}
	priceTable, err := loadPriceTable(opts.PriceTablePath, opts.Logger)
	if err != nil {
		return nil, nil, err
	}

	dbPath := resolveStateDBPath(opts.StateDB, opts.CacheDir)
	if err := os.MkdirAll(filepath.Dir(dbPath), format.DirPerm); err != nil {
		return nil, nil, fmt.Errorf("ensure state-db parent dir: %w", err)
	}
	// SQLite open + ping is fast and shouldn't be interrupted by a
	// caller cancelling startup — context.WithoutCancel preserves any
	// logger/value the parent carries while dropping cancellation.
	db, err := OpenStateDB(context.WithoutCancel(ctx), dbPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open state db: %w", err)
	}
	opts.Logger.Info().Str("path", dbPath).Msg("opened state db")

	cleanup := func() {
		if cerr := db.Close(); cerr != nil {
			opts.Logger.Error().Err(cerr).Msg("close state db")
		}
	}

	if err := migrate.Apply(db); err != nil {
		cleanup()

		return nil, nil, fmt.Errorf("apply migrations: %w", err)
	}
	versionInfo, err := migrate.Version(db)
	if err != nil {
		cleanup()

		return nil, nil, fmt.Errorf("read schema version: %w", err)
	}
	opts.Logger.Info().Uint("schema_version", versionInfo.Version).Bool("dirty", versionInfo.Dirty).Msg("schema migrated")

	srv, err := New(ctx, Config{
		Addr:                     opts.Addr,
		Logger:                   opts.Logger,
		PriceTable:               priceTable,
		S3Source:                 opts.S3Source,
		CacheDir:                 opts.CacheDir,
		DB:                       db,
		AutoLoad:                 opts.AutoLoad,
		PollInterval:             opts.PollInterval,
		MaxIndexDisk:             opts.MaxIndexDisk,
		IndexHeadroomBytes:       opts.IndexHeadroomBytes,
		AutoLoadConcurrency:      opts.AutoLoadConcurrency,
		AutoLoadRetentionDefault: opts.AutoLoadRetentionDefault,
		IndexRatio:               opts.IndexRatio,
		DiscoveryRefreshInterval: opts.DiscoveryRefreshInterval,
		QueryBatchMax:            opts.QueryBatchMax,
		MetricsAddr:              opts.MetricsAddr,
		AutoLoadDryRun:           opts.AutoLoadDryRun,
	})
	if err != nil {
		cleanup()

		return nil, nil, fmt.Errorf("create server: %w", err)
	}
	if err := applyInventoryConfigs(ctx, srv.configStore, opts.InventoryConfigs); err != nil {
		cleanup()

		return nil, nil, fmt.Errorf("apply inventory configs: %w", err)
	}

	return srv, cleanup, nil
}

func applyInventoryConfigs(ctx context.Context, store *inventory.ConfigStore, entries []InventoryConfigEntry) error {
	if store == nil || len(entries) == 0 {
		return nil
	}
	for i := range entries {
		e := &entries[i]
		existing, err := store.Get(ctx, e.Source, e.Name)
		switch {
		case err == nil:
			existing.AutoLoad = e.AutoLoad
			if e.RetentionCount > 0 {
				existing.RetentionCount = e.RetentionCount
			}
			if err := store.Upsert(ctx, existing); err != nil {
				return fmt.Errorf("update %s/%s: %w", e.Source, e.Name, err)
			}

			continue
		case errors.Is(err, inventory.ErrStoreNotFound):
			// fall through to insert path below
		default:
			return fmt.Errorf("get %s/%s: %w", e.Source, e.Name, err)
		}
		retention := e.RetentionCount
		if retention == 0 {
			retention = inventory.DefaultRetentionCount
		}
		if err := store.Upsert(ctx, inventory.Config{
			Source:         e.Source,
			Name:           e.Name,
			AutoLoad:       e.AutoLoad,
			RetentionCount: retention,
		}); err != nil {
			return fmt.Errorf("insert %s/%s: %w", e.Source, e.Name, err)
		}
	}

	return nil
}

// BootstrapAndRun is the full happy-path of the binary, factored out so
// the main goroutine is just signal handling and exit-code reporting.
// The S3 client probe inside Bootstrap uses its own bounded context
// (see newDiscoveryWiring) so callers don't need to budget for it.
func BootstrapAndRun(ctx context.Context, opts RuntimeOptions) error {
	srv, cleanup, err := Bootstrap(ctx, opts)
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
