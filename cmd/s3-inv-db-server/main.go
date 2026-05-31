// Command s3-inv-db-server starts an HTTP server for querying S3 inventory indexes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/internal/autoload"
	"github.com/eunmann/s3-inv-db/internal/server"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// errNegativeSize is returned when a human size string parses to a negative value.
var errNegativeSize = errors.New("negative size")

// Server-flag defaults split out as constants so the call-site reads
// declaratively and `mnd` lint stops flagging the literals.
const (
	defaultIndexRatio        = 0.30
	defaultAutoLoadRetention = 2
	headroomDivisor          = 5
	// Power-of-two byte multipliers for "KiB/MiB/GiB/TiB" suffixes.
	bitsPerTebibyte = 40
	bitsPerGibibyte = 30
	bitsPerMebibyte = 20
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// serverFlags collects the raw flag *values for run(). Filled by
// defineFlags before flag.Parse; resolveRuntimeOptions merges them with
// file/env config into the final server.RuntimeOptions.
type serverFlags struct {
	configPath        *string
	addr              *string
	verbose           *bool
	prettyLogs        *bool
	priceTablePath    *string
	s3Source          *string
	cacheDir          *string
	stateDB           *string
	autoLoad          *bool
	pollInterval      *time.Duration
	discoveryRefresh  *time.Duration
	maxIndexDisk      *string
	headroom          *string
	maxConcurrentJobs *int
	autoLoadRetention *uint
	indexRatio        *float64
	queryBatchMax     *int
	metricsAddr       *string
	autoLoadDryRun    *bool
}

func defineFlags(fs *flag.FlagSet) *serverFlags {
	return &serverFlags{
		configPath:        fs.String("config", "", "path to JSON config file (overridden by explicit flags)"),
		addr:              fs.String("addr", ":8080", "HTTP server address"),
		verbose:           fs.Bool("verbose", false, "enable debug logging"),
		prettyLogs:        fs.Bool("pretty-logs", false, "use human-friendly console output"),
		priceTablePath:    fs.String("price-table", "", "path to custom price table JSON (default: US East 1 prices)"),
		s3Source:          fs.String("s3-source", "", "S3 URI to discover inventories under (e.g., s3://bucket/inventory-data/)"),
		cacheDir:          fs.String("cache-dir", "/var/cache/s3inv", "local directory for built indexes downloaded from S3"),
		stateDB:           fs.String("state-db", "", "SQLite path for persisted state (default: <cache-dir>/state.db)"),
		autoLoad:          fs.Bool("auto-load", false, "enable background discovery + auto-load of new inventory runs; requires --max-index-disk"),
		pollInterval:      fs.Duration("auto-load-poll-interval", autoload.DefaultPollInterval, "discovery polling interval"),
		discoveryRefresh:  fs.Duration("discovery-refresh-interval", server.DefaultDiscoveryRefreshInterval, "interval at which the background discovery refresher updates the cached snapshot served by HTTP handlers"),
		maxIndexDisk:      fs.String("max-index-disk", "", "max cumulative on-disk bytes for loaded indexes (e.g. 100GB); required with --auto-load"),
		headroom:          fs.String("index-headroom", "", "reserved unused space inside --max-index-disk; default 20% of the cap"),
		maxConcurrentJobs: fs.Int("max-concurrent-jobs", 1, "max jobs (auto-loads and manual builds) running at once"),
		autoLoadRetention: fs.Uint("auto-load-retention-default", defaultAutoLoadRetention, "default per-config run-retention when a configuration sets none"),
		indexRatio:        fs.Float64("index-ratio", defaultIndexRatio, "estimate multiplier: final index bytes ≈ ratio × compressed manifest total"),
		queryBatchMax:     fs.Int("query-batch-max", 0, "max prefixes per batch stats request (0 = handler default)"),
		metricsAddr:       fs.String("metrics-addr", "", "bind /metrics on this address; empty = mount on the main listener"),
		autoLoadDryRun:    fs.Bool("auto-load-dry-run", false, "log autoload decisions instead of acting on them"),
	}
}

func resolveRuntimeOptions(f *serverFlags, fileCfg *appconfig.Config, explicit map[string]bool, logger zerolog.Logger) (server.RuntimeOptions, error) {
	capStr := pickString(fileCfg, *f.maxIndexDisk, explicit["max-index-disk"], func(c *appconfig.Config) *string { return c.MaxIndexDisk })
	headStr := pickString(fileCfg, *f.headroom, explicit["index-headroom"], func(c *appconfig.Config) *string { return c.IndexHeadroom })
	capBytes, err := parseSize(capStr)
	if err != nil {
		return server.RuntimeOptions{}, fmt.Errorf("max_index_disk: %w", err)
	}
	headBytes, err := parseSize(headStr)
	if err != nil {
		return server.RuntimeOptions{}, fmt.Errorf("index_headroom: %w", err)
	}
	if capBytes > 0 && headBytes == 0 {
		headBytes = capBytes / headroomDivisor
	}
	finalInterval, err := resolveDuration(*f.pollInterval, explicit["auto-load-poll-interval"], appconfig.FromFile(fileCfg, func(c *appconfig.Config) *string { return c.PollInterval }))
	if err != nil {
		return server.RuntimeOptions{}, fmt.Errorf("auto_load_poll_interval: %w", err)
	}
	finalDiscoveryRefresh, err := resolveDuration(*f.discoveryRefresh, explicit["discovery-refresh-interval"], appconfig.FromFile(fileCfg, func(c *appconfig.Config) *string { return c.DiscoveryRefreshInterval }))
	if err != nil {
		return server.RuntimeOptions{}, fmt.Errorf("discovery_refresh_interval: %w", err)
	}

	return server.RuntimeOptions{
		Addr:                     pickString(fileCfg, *f.addr, explicit["addr"], func(c *appconfig.Config) *string { return c.Addr }),
		S3Source:                 pickString(fileCfg, *f.s3Source, explicit["s3-source"], func(c *appconfig.Config) *string { return c.S3Source }),
		CacheDir:                 pickString(fileCfg, *f.cacheDir, explicit["cache-dir"], func(c *appconfig.Config) *string { return c.CacheDir }),
		StateDB:                  pickString(fileCfg, *f.stateDB, explicit["state-db"], func(c *appconfig.Config) *string { return c.StateDB }),
		PriceTablePath:           pickString(fileCfg, *f.priceTablePath, explicit["price-table"], func(c *appconfig.Config) *string { return c.PriceTable }),
		AutoLoad:                 pickBool(fileCfg, *f.autoLoad, explicit["auto-load"], func(c *appconfig.Config) *bool { return c.AutoLoad }),
		PollInterval:             finalInterval,
		DiscoveryRefreshInterval: finalDiscoveryRefresh,
		MaxIndexDisk:             capBytes,
		IndexHeadroomBytes:       headBytes,
		MaxConcurrentJobs:        appconfig.Pick(*f.maxConcurrentJobs, explicit["max-concurrent-jobs"], appconfig.FromFile(fileCfg, func(c *appconfig.Config) *int { return c.MaxConcurrentJobs })),
		AutoLoadRetentionDefault: appconfig.Pick(uint32(*f.autoLoadRetention), explicit["auto-load-retention-default"], appconfig.FromFile(fileCfg, func(c *appconfig.Config) *uint32 { return c.AutoLoadRetentionDefault })),
		IndexRatio:               appconfig.Pick(*f.indexRatio, explicit["index-ratio"], appconfig.FromFile(fileCfg, func(c *appconfig.Config) *float64 { return c.IndexRatio })),
		QueryBatchMax:            appconfig.Pick(*f.queryBatchMax, explicit["query-batch-max"], appconfig.FromFile(fileCfg, func(c *appconfig.Config) *int { return c.QueryBatchMax })),
		MetricsAddr:              pickString(fileCfg, *f.metricsAddr, explicit["metrics-addr"], func(c *appconfig.Config) *string { return c.MetricsAddr }),
		AutoLoadDryRun:           pickBool(fileCfg, *f.autoLoadDryRun, explicit["auto-load-dry-run"], func(c *appconfig.Config) *bool { return c.AutoLoadDryRun }),
		InventoryConfigs:         inventoryConfigsFromFile(fileCfg),
		Logger:                   logger,
	}, nil
}

func run() error {
	fs := flag.NewFlagSet("s3-inv-db-server", flag.ExitOnError)
	f := defineFlags(fs)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	fileCfg, err := appconfig.Load(*f.configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	explicit := map[string]bool{}
	fs.Visit(func(fl *flag.Flag) { explicit[fl.Name] = true })

	finalVerbose := pickBool(fileCfg, *f.verbose, explicit["verbose"], func(c *appconfig.Config) *bool { return c.Verbose })
	finalPretty := pickBool(fileCfg, *f.prettyLogs, explicit["pretty-logs"], func(c *appconfig.Config) *bool { return c.PrettyLogs })

	logger := logging.NewLogger(logging.Options{Debug: finalVerbose, Human: finalPretty})
	log.Logger = logger

	memLimit := sysmem.ApplyMemoryLimit(sysmem.DefaultMemoryLimitFraction)
	logger.Info().
		Int64("bytes", memLimit.Bytes).
		Str("source", string(memLimit.Source)).
		Int64("env_bytes", memLimit.EnvBytes).
		Int64("cgroup_bytes", memLimit.CgroupBytes).
		Int64("sysmem_fraction_bytes", memLimit.SysmemFractionBytes).
		Msg("process memory limit configured")

	opts, err := resolveRuntimeOptions(f, fileCfg, explicit, logger)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.BootstrapAndRun(ctx, opts); err != nil {
		return fmt.Errorf("server: %w", err)
	}

	return nil
}

func pickString(cfg *appconfig.Config, flagVal string, explicit bool, get func(*appconfig.Config) *string) string {
	var p *string
	if cfg != nil {
		p = get(cfg)
	}

	return appconfig.Pick(flagVal, explicit, p)
}

func pickBool(cfg *appconfig.Config, flagVal, explicit bool, get func(*appconfig.Config) *bool) bool {
	var p *bool
	if cfg != nil {
		p = get(cfg)
	}

	return appconfig.Pick(flagVal, explicit, p)
}

func resolveDuration(flagVal time.Duration, explicit bool, configVal *string) (time.Duration, error) {
	if explicit {
		return flagVal, nil
	}
	if configVal != nil {
		d, err := time.ParseDuration(*configVal)
		if err != nil {
			return 0, fmt.Errorf("parse duration %q: %w", *configVal, err)
		}

		return d, nil
	}

	return flagVal, nil
}

func inventoryConfigsFromFile(cfg *appconfig.Config) []server.InventoryConfigEntry {
	if cfg == nil || len(cfg.Inventories) == 0 {
		return nil
	}
	out := make([]server.InventoryConfigEntry, 0, len(cfg.Inventories))
	for i := range cfg.Inventories {
		e := &cfg.Inventories[i]
		out = append(out, server.InventoryConfigEntry{
			Source:         e.Source,
			Name:           e.Name,
			AutoLoad:       e.AutoLoad,
			RetentionCount: e.RetentionCount,
		})
	}

	return out
}

// parseSize accepts "", a raw byte count, or a number with a suffix
// (KB/MB/GB/TB and KiB/MiB/GiB/TiB) and returns bytes.
func parseSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	var mult uint64 = 1
	upper := strings.ToUpper(s)
	switch {
	case strings.HasSuffix(upper, "TIB"):
		mult, s = 1<<bitsPerTebibyte, s[:len(s)-3]
	case strings.HasSuffix(upper, "GIB"):
		mult, s = 1<<bitsPerGibibyte, s[:len(s)-3]
	case strings.HasSuffix(upper, "MIB"):
		mult, s = 1<<bitsPerMebibyte, s[:len(s)-3]
	case strings.HasSuffix(upper, "KIB"):
		mult, s = 1<<10, s[:len(s)-3]
	case strings.HasSuffix(upper, "TB"):
		mult, s = 1e12, s[:len(s)-2]
	case strings.HasSuffix(upper, "GB"):
		mult, s = 1e9, s[:len(s)-2]
	case strings.HasSuffix(upper, "MB"):
		mult, s = 1e6, s[:len(s)-2]
	case strings.HasSuffix(upper, "KB"):
		mult, s = 1e3, s[:len(s)-2]
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("%w %q", errNegativeSize, s)
	}

	return uint64(n * float64(mult)), nil
}
