// Command s3inv-server starts an HTTP server for querying S3 inventory indexes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/eunmann/s3-inv-db/internal/logctx"
	"github.com/eunmann/s3-inv-db/internal/server"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
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
	devMode := flag.Bool("dev", false, "development mode (reload templates on each request)")
	priceTablePath := flag.String("price-table", "", "path to custom price table JSON (default: US East 1 prices)")

	flag.Parse()

	logger := logctx.NewConfiguredLogger(*verbose, *prettyLogs)
	logctx.SetDefaultLogger(logger)

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
		DevMode:    *devMode,
		PriceTable: priceTable,
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
