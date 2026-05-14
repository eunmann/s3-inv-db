// Command s3-inv-db-server starts an HTTP server for querying S3 inventory indexes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/eunmann/s3-inv-db/internal/server"
	"github.com/eunmann/s3-inv-db/pkg/logging"
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
	stateDB := flag.String("state-db", envOr("S3INV_STATE_DB", ""), "SQLite path for persisted state (default: <cache-dir>/state.db)")
	flag.Parse()

	logger := logging.NewLogger(*verbose, *prettyLogs)
	log.Logger = logger // zerolog/log package-global for code paths without a ctx

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.BootstrapAndRun(ctx, server.RuntimeOptions{
		Addr:           *addr,
		S3Source:       *s3Source,
		CacheDir:       *cacheDir,
		StateDB:        *stateDB,
		PriceTablePath: *priceTablePath,
		Logger:         logger,
	}); err != nil {
		return fmt.Errorf("server: %w", err)
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
