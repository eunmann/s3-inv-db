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
	"github.com/rs/zerolog/log"
)

// ErrNegativeSize is returned when a human size string parses to a negative value.
var ErrNegativeSize = errors.New("negative size")

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

func run() error {
	configPath := flag.String("config", appconfig.EnvOr("S3INV_CONFIG", ""), "path to JSON config file (overridden by explicit flags)")
	addr := flag.String("addr", ":8080", "HTTP server address")
	verbose := flag.Bool("verbose", false, "enable debug logging")
	prettyLogs := flag.Bool("pretty-logs", false, "use human-friendly console output")
	priceTablePath := flag.String("price-table", "", "path to custom price table JSON (default: US East 1 prices)")
	s3Source := flag.String("s3-source", appconfig.EnvOr("S3INV_SOURCE", ""), "S3 URI to discover inventories under (e.g., s3://bucket/inventory-data/)")
	cacheDir := flag.String("cache-dir", appconfig.EnvOr("S3INV_CACHE_DIR", "/var/cache/s3inv"), "local directory for built indexes downloaded from S3")
	stateDB := flag.String("state-db", appconfig.EnvOr("S3INV_STATE_DB", ""), "SQLite path for persisted state (default: <cache-dir>/state.db)")

	autoLoad := flag.Bool("auto-load", appconfig.EnvBool("S3INV_AUTO_LOAD", false), "enable background discovery + auto-load of new inventory runs; requires --max-index-disk")
	pollInterval := flag.Duration("auto-load-poll-interval", appconfig.EnvDuration("S3INV_AUTO_LOAD_POLL_INTERVAL", autoload.DefaultPollInterval), "discovery polling interval")
	discoveryRefresh := flag.Duration("discovery-refresh-interval", appconfig.EnvDuration("S3INV_DISCOVERY_REFRESH_INTERVAL", server.DefaultDiscoveryRefreshInterval), "interval at which the background discovery refresher updates the cached snapshot served by HTTP handlers")
	maxIndexDisk := flag.String("max-index-disk", appconfig.EnvOr("S3INV_MAX_INDEX_DISK", ""), "max cumulative on-disk bytes for loaded indexes (e.g. 100GB); required with --auto-load")
	headroom := flag.String("index-headroom", appconfig.EnvOr("S3INV_INDEX_HEADROOM", ""), "reserved unused space inside --max-index-disk; default 20% of the cap")
	autoLoadConcurrency := flag.Int("max-auto-load-concurrency", appconfig.EnvInt("S3INV_MAX_AUTO_LOAD_CONCURRENCY", 1), "max concurrent auto-loads")
	autoLoadRetention := flag.Uint("auto-load-retention-default", uint(appconfig.EnvInt("S3INV_AUTO_LOAD_RETENTION_DEFAULT", defaultAutoLoadRetention)), "default per-config run-retention when a configuration sets none")
	indexRatio := flag.Float64("index-ratio", appconfig.EnvFloat("S3INV_INDEX_RATIO", defaultIndexRatio), "estimate multiplier: final index bytes ≈ ratio × compressed manifest total")
	flag.Parse()

	fileCfg, err := appconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	explicit := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	finalAddr := pickString(fileCfg, *addr, explicit["addr"], func(c *appconfig.Config) *string { return c.Addr })
	finalVerbose := pickBool(fileCfg, *verbose, explicit["verbose"], func(c *appconfig.Config) *bool { return c.Verbose })
	finalPretty := pickBool(fileCfg, *prettyLogs, explicit["pretty-logs"], func(c *appconfig.Config) *bool { return c.PrettyLogs })
	finalPrice := pickString(fileCfg, *priceTablePath, explicit["price-table"], func(c *appconfig.Config) *string { return c.PriceTable })
	finalSrc := pickString(fileCfg, *s3Source, explicit["s3-source"], func(c *appconfig.Config) *string { return c.S3Source })
	finalCache := pickString(fileCfg, *cacheDir, explicit["cache-dir"], func(c *appconfig.Config) *string { return c.CacheDir })
	finalState := pickString(fileCfg, *stateDB, explicit["state-db"], func(c *appconfig.Config) *string { return c.StateDB })
	finalAuto := pickBool(fileCfg, *autoLoad, explicit["auto-load"], func(c *appconfig.Config) *bool { return c.AutoLoad })
	finalConc := appconfig.PickInt(*autoLoadConcurrency, explicit["max-auto-load-concurrency"], fileConfigInt(fileCfg, func(c *appconfig.Config) *int { return c.AutoLoadConcurrency }))
	finalRet := appconfig.PickUint32(uint32(*autoLoadRetention), explicit["auto-load-retention-default"], fileConfigUint32(fileCfg, func(c *appconfig.Config) *uint32 { return c.AutoLoadRetentionDefault }))
	finalRatio := appconfig.PickFloat64(*indexRatio, explicit["index-ratio"], fileConfigFloat(fileCfg, func(c *appconfig.Config) *float64 { return c.IndexRatio }))

	finalInterval, err := resolveDuration(*pollInterval, explicit["auto-load-poll-interval"], fileConfigString(fileCfg, func(c *appconfig.Config) *string { return c.PollInterval }))
	if err != nil {
		return fmt.Errorf("auto_load_poll_interval: %w", err)
	}
	finalDiscoveryRefresh, err := resolveDuration(*discoveryRefresh, explicit["discovery-refresh-interval"], fileConfigString(fileCfg, func(c *appconfig.Config) *string { return c.DiscoveryRefreshInterval }))
	if err != nil {
		return fmt.Errorf("discovery_refresh_interval: %w", err)
	}

	capStr := pickString(fileCfg, *maxIndexDisk, explicit["max-index-disk"], func(c *appconfig.Config) *string { return c.MaxIndexDisk })
	headStr := pickString(fileCfg, *headroom, explicit["index-headroom"], func(c *appconfig.Config) *string { return c.IndexHeadroom })
	capBytes, err := parseSize(capStr)
	if err != nil {
		return fmt.Errorf("max_index_disk: %w", err)
	}
	headBytes, err := parseSize(headStr)
	if err != nil {
		return fmt.Errorf("index_headroom: %w", err)
	}
	if capBytes > 0 && headBytes == 0 {
		headBytes = capBytes / headroomDivisor
	}

	logger := logging.NewLogger(finalVerbose, finalPretty)
	log.Logger = logger

	memLimit := sysmem.ApplyMemoryLimit(sysmem.DefaultMemoryLimitFraction)
	logger.Info().
		Int64("bytes", memLimit.Bytes).
		Str("source", string(memLimit.Source)).
		Int64("env_bytes", memLimit.EnvBytes).
		Int64("cgroup_bytes", memLimit.CgroupBytes).
		Int64("sysmem_fraction_bytes", memLimit.SysmemFractionBytes).
		Msg("process memory limit configured")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.BootstrapAndRun(ctx, server.RuntimeOptions{
		Addr:                     finalAddr,
		S3Source:                 finalSrc,
		CacheDir:                 finalCache,
		StateDB:                  finalState,
		PriceTablePath:           finalPrice,
		AutoLoad:                 finalAuto,
		PollInterval:             finalInterval,
		DiscoveryRefreshInterval: finalDiscoveryRefresh,
		MaxIndexDisk:             capBytes,
		IndexHeadroomBytes:       headBytes,
		AutoLoadConcurrency:      finalConc,
		AutoLoadRetentionDefault: finalRet,
		IndexRatio:               finalRatio,
		InventoryConfigs:         inventoryConfigsFromFile(fileCfg),
		Logger:                   logger,
	}); err != nil {
		return fmt.Errorf("server: %w", err)
	}

	return nil
}

func pickString(cfg *appconfig.Config, flagVal string, explicit bool, get func(*appconfig.Config) *string) string {
	var p *string
	if cfg != nil {
		p = get(cfg)
	}

	return appconfig.PickString(flagVal, explicit, p)
}

func pickBool(cfg *appconfig.Config, flagVal, explicit bool, get func(*appconfig.Config) *bool) bool {
	var p *bool
	if cfg != nil {
		p = get(cfg)
	}

	return appconfig.PickBool(flagVal, explicit, p)
}

func fileConfigInt(cfg *appconfig.Config, get func(*appconfig.Config) *int) *int {
	if cfg == nil {
		return nil
	}

	return get(cfg)
}

func fileConfigUint32(cfg *appconfig.Config, get func(*appconfig.Config) *uint32) *uint32 {
	if cfg == nil {
		return nil
	}

	return get(cfg)
}

func fileConfigFloat(cfg *appconfig.Config, get func(*appconfig.Config) *float64) *float64 {
	if cfg == nil {
		return nil
	}

	return get(cfg)
}

func fileConfigString(cfg *appconfig.Config, get func(*appconfig.Config) *string) *string {
	if cfg == nil {
		return nil
	}

	return get(cfg)
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
		return 0, fmt.Errorf("%w %q", ErrNegativeSize, s)
	}

	return uint64(n * float64(mult)), nil
}
