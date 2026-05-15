// Command s3-inv-db-server starts an HTTP server for querying S3 inventory indexes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/server"
	"github.com/rs/zerolog/log"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", ":8080", "HTTP server address")
	verbose := flag.Bool("verbose", false, "enable debug logging")
	prettyLogs := flag.Bool("pretty-logs", false, "use human-friendly console output")
	priceTablePath := flag.String("price-table", "", "path to custom price table JSON (default: US East 1 prices)")
	s3Source := flag.String("s3-source", envOr("S3INV_SOURCE", ""), "S3 URI to discover inventories under (e.g., s3://bucket/inventory-data/)")
	cacheDir := flag.String("cache-dir", envOr("S3INV_CACHE_DIR", "/var/cache/s3inv"), "local directory for built indexes downloaded from S3")
	scratchDir := flag.String("scratch-dir", envOr("S3INV_SCRATCH_DIR", ""), "directory for transient load-time files; defaults to --cache-dir")
	stateDB := flag.String("state-db", envOr("S3INV_STATE_DB", ""), "SQLite path for persisted state (default: <cache-dir>/state.db)")

	autoLoad := flag.Bool("auto-load", envBool("S3INV_AUTO_LOAD", false), "enable background discovery + auto-load of new inventory runs; requires --max-index-disk")
	pollInterval := flag.Duration("auto-load-poll-interval", envDuration("S3INV_AUTO_LOAD_POLL_INTERVAL", 15*time.Minute), "discovery polling interval")
	maxIndexDisk := flag.String("max-index-disk", envOr("S3INV_MAX_INDEX_DISK", ""), "max cumulative on-disk bytes for loaded indexes (e.g. 100GB); required with --auto-load")
	headroom := flag.String("index-headroom", envOr("S3INV_INDEX_HEADROOM", ""), "reserved unused space inside --max-index-disk; default 20% of the cap")
	autoLoadConcurrency := flag.Int("max-auto-load-concurrency", envInt("S3INV_MAX_AUTO_LOAD_CONCURRENCY", 1), "max concurrent auto-loads")
	autoLoadRetention := flag.Uint("auto-load-retention-default", uint(envInt("S3INV_AUTO_LOAD_RETENTION_DEFAULT", 2)), "default per-config run-retention when a configuration sets none")
	indexRatio := flag.Float64("index-ratio", envFloat("S3INV_INDEX_RATIO", 0.30), "estimate multiplier: final index bytes ≈ ratio × compressed manifest total")
	flag.Parse()

	capBytes, err := parseSize(*maxIndexDisk)
	if err != nil {
		return fmt.Errorf("--max-index-disk: %w", err)
	}
	headBytes, err := parseSize(*headroom)
	if err != nil {
		return fmt.Errorf("--index-headroom: %w", err)
	}
	if capBytes > 0 && headBytes == 0 {
		headBytes = capBytes / 5
	}

	logger := logging.NewLogger(*verbose, *prettyLogs)
	log.Logger = logger // zerolog/log package-global for code paths without a ctx

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.BootstrapAndRun(ctx, server.RuntimeOptions{
		Addr:                     *addr,
		S3Source:                 *s3Source,
		CacheDir:                 *cacheDir,
		ScratchDir:               *scratchDir,
		StateDB:                  *stateDB,
		PriceTablePath:           *priceTablePath,
		AutoLoad:                 *autoLoad,
		PollInterval:             *pollInterval,
		MaxIndexDisk:             capBytes,
		IndexHeadroomBytes:       headBytes,
		AutoLoadConcurrency:      *autoLoadConcurrency,
		AutoLoadRetentionDefault: uint32(*autoLoadRetention),
		IndexRatio:               *indexRatio,
		Logger:                   logger,
	}); err != nil {
		return fmt.Errorf("server: %w", err)
	}
	return nil
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
		mult, s = 1<<40, s[:len(s)-3]
	case strings.HasSuffix(upper, "GIB"):
		mult, s = 1<<30, s[:len(s)-3]
	case strings.HasSuffix(upper, "MIB"):
		mult, s = 1<<20, s[:len(s)-3]
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
		return 0, fmt.Errorf("negative size %q", s)
	}
	return uint64(n * float64(mult)), nil
}

func envBool(k string, def bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envFloat(k string, def float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func envDuration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
