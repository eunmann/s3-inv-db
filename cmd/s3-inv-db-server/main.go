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
	defaultAutoLoadRetention = 2
	// HeadroomDivisor: 1/headroomDivisor of MaxIndexDisk is reserved as
	// unused buffer so loads that exceed their size estimate have room.
	headroomDivisor = 5
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
// file config into the final server.RuntimeOptions.
type serverFlags struct {
	configPath        *string
	addr              *string
	verbose           *bool
	prettyLogs        *bool
	priceTablePath    *string
	s3Source          *string
	cacheDir          *string
	autoLoad          *bool
	pollInterval      *time.Duration
	discoveryRefresh  *time.Duration
	maxIndexDisk      *string
	maxConcurrentJobs *int
	autoLoadRetention *uint
	queryBatchMax     *int
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
		autoLoad:          fs.Bool("auto-load", false, "enable background discovery + auto-load of new inventory runs; requires --max-index-disk"),
		pollInterval:      fs.Duration("auto-load-poll-interval", autoload.DefaultPollInterval, "discovery polling interval"),
		discoveryRefresh:  fs.Duration("discovery-refresh-interval", server.DefaultDiscoveryRefreshInterval, "interval at which the background discovery refresher updates the cached snapshot served by HTTP handlers"),
		maxIndexDisk:      fs.String("max-index-disk", "", "max cumulative on-disk bytes for loaded indexes (e.g. 100GB); required with --auto-load"),
		maxConcurrentJobs: fs.Int("max-concurrent-jobs", 1, "max jobs (auto-loads and manual builds) running at once"),
		autoLoadRetention: fs.Uint("auto-load-retention-default", defaultAutoLoadRetention, "default per-config run-retention when a configuration sets none"),
		queryBatchMax:     fs.Int("query-batch-max", 0, "max prefixes per batch stats request (0 = handler default)"),
	}
}

func resolveRuntimeOptions(f *serverFlags, fileCfg *appconfig.Config, explicit map[string]bool, logger zerolog.Logger) (server.RuntimeOptions, error) {
	capStr := appconfig.PickFile(*f.maxIndexDisk, explicit["max-index-disk"], fileCfg, func(c *appconfig.Config) *string { return c.MaxIndexDisk })
	capBytes, err := parseSize(capStr)
	if err != nil {
		return server.RuntimeOptions{}, fmt.Errorf("max_index_disk: %w", err)
	}
	finalInterval, err := resolveDuration(*f.pollInterval, explicit["auto-load-poll-interval"], fileCfg, func(c *appconfig.Config) *string { return c.PollInterval })
	if err != nil {
		return server.RuntimeOptions{}, fmt.Errorf("auto_load_poll_interval: %w", err)
	}
	finalDiscoveryRefresh, err := resolveDuration(*f.discoveryRefresh, explicit["discovery-refresh-interval"], fileCfg, func(c *appconfig.Config) *string { return c.DiscoveryRefreshInterval })
	if err != nil {
		return server.RuntimeOptions{}, fmt.Errorf("discovery_refresh_interval: %w", err)
	}

	return server.RuntimeOptions{
		Addr:                     appconfig.PickFile(*f.addr, explicit["addr"], fileCfg, func(c *appconfig.Config) *string { return c.Addr }),
		S3Source:                 appconfig.PickFile(*f.s3Source, explicit["s3-source"], fileCfg, func(c *appconfig.Config) *string { return c.S3Source }),
		CacheDir:                 appconfig.PickFile(*f.cacheDir, explicit["cache-dir"], fileCfg, func(c *appconfig.Config) *string { return c.CacheDir }),
		PriceTablePath:           appconfig.PickFile(*f.priceTablePath, explicit["price-table"], fileCfg, func(c *appconfig.Config) *string { return c.PriceTable }),
		AutoLoad:                 appconfig.PickFile(*f.autoLoad, explicit["auto-load"], fileCfg, func(c *appconfig.Config) *bool { return c.AutoLoad }),
		PollInterval:             finalInterval,
		DiscoveryRefreshInterval: finalDiscoveryRefresh,
		MaxIndexDisk:             capBytes,
		IndexHeadroomBytes:       capBytes / headroomDivisor,
		MaxConcurrentJobs:        appconfig.PickFile(*f.maxConcurrentJobs, explicit["max-concurrent-jobs"], fileCfg, func(c *appconfig.Config) *int { return c.MaxConcurrentJobs }),
		AutoLoadRetentionDefault: appconfig.PickFile(uint32(*f.autoLoadRetention), explicit["auto-load-retention-default"], fileCfg, func(c *appconfig.Config) *uint32 { return c.AutoLoadRetentionDefault }),
		QueryBatchMax:            appconfig.PickFile(*f.queryBatchMax, explicit["query-batch-max"], fileCfg, func(c *appconfig.Config) *int { return c.QueryBatchMax }),
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

	finalVerbose := appconfig.PickFile(*f.verbose, explicit["verbose"], fileCfg, func(c *appconfig.Config) *bool { return c.Verbose })
	finalPretty := appconfig.PickFile(*f.prettyLogs, explicit["pretty-logs"], fileCfg, func(c *appconfig.Config) *bool { return c.PrettyLogs })

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

func resolveDuration(flagVal time.Duration, explicit bool, cfg *appconfig.Config, get func(*appconfig.Config) *string) (time.Duration, error) {
	if explicit {
		return flagVal, nil
	}
	if cfg != nil {
		if p := get(cfg); p != nil {
			d, err := time.ParseDuration(*p)
			if err != nil {
				return 0, fmt.Errorf("parse duration %q: %w", *p, err)
			}

			return d, nil
		}
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
