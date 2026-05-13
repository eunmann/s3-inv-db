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
	"github.com/eunmann/s3-inv-db/pkg/pricing"
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

	flag.Parse()

	logger := logging.NewLogger(*verbose, *prettyLogs)
	log.Logger = logger // zerolog/log package-global for code paths without a ctx

	// Load price table
	var priceTable pricing.PriceTable
	if *priceTablePath != "" {
		var err error
		priceTable, err = pricing.LoadPriceTable(*priceTablePath)
		if err != nil {
			return fmt.Errorf("load price table: %w", err)
		}
		logger.Info().Str("path", *priceTablePath).Msg("loaded custom price table")
	} else {
		priceTable = pricing.DefaultUSEast1Prices()
	}

	cfg := server.Config{
		Addr:       *addr,
		Logger:     logger,
		PriceTable: priceTable,
		S3Source:   *s3Source,
		CacheDir:   *cacheDir,
	}

	srv, err := server.New(cfg)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil {
		return fmt.Errorf("run server: %w", err)
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
