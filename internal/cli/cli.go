// Package cli implements the command-line interface for s3inv-index.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/eunmann/s3-inv-db/pkg/indexbuild"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
	"github.com/rs/zerolog"
)

// Run executes the CLI with the given arguments.
func Run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: s3inv-index <command> [options]\ncommands: build, query")
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "build":
		return runBuild(cmdArgs)
	case "query":
		return runQuery(cmdArgs)
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func runBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	outDir := fs.String("out", "", "output directory for index files")
	s3Manifest := fs.String("s3-manifest", "", "S3 URI to inventory manifest.json (s3://bucket/path/manifest.json)")
	dbPath := fs.String("db", "", "path to SQLite database for aggregation (default: <out>.db)")
	backend := fs.String("backend", "sqlite", "build backend: 'sqlite' (default, uses CGO) or 'extsort' (pure Go, external sort)")
	verbose := fs.Bool("verbose", false, "enable debug level logging")
	prettyLogs := fs.Bool("pretty-logs", false, "use human-friendly console output")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	// Initialize logging based on flags
	logging.Init(*verbose, *prettyLogs)
	log := logging.L()

	if *outDir == "" {
		return errors.New("--out is required")
	}
	if *s3Manifest == "" {
		return errors.New("--s3-manifest is required")
	}

	// Dispatch to appropriate backend
	switch *backend {
	case "sqlite":
		return runBuildSQLite(log, *outDir, *s3Manifest, *dbPath)
	case "extsort":
		return runBuildExtSort(log, *outDir, *s3Manifest)
	default:
		return fmt.Errorf("unknown backend: %s (use 'sqlite' or 'extsort')", *backend)
	}
}

// runBuildSQLite runs the build using the SQLite-based backend (original implementation).
func runBuildSQLite(log *zerolog.Logger, outDir, s3Manifest, dbPath string) error {
	ctx := context.Background()

	// Use default DB path if not specified
	if dbPath == "" {
		dbPath = outDir + ".db"
	}

	log.Info().
		Str("phase", "build_start").
		Str("backend", "sqlite").
		Str("s3_manifest", s3Manifest).
		Str("output_dir", outDir).
		Str("db_path", dbPath).
		Msg("starting S3 inventory build")

	// Create S3 client
	client, err := s3fetch.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	// Remove any existing database to ensure clean build
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove existing database: %w", err)
	}

	// Stream from S3 using fast in-memory aggregation
	memCfg := sqliteagg.MemoryStreamConfig{
		ManifestURI:  s3Manifest,
		DBPath:       dbPath,
		MemoryConfig: sqliteagg.DefaultMemoryAggregatorConfig(dbPath),
	}
	result, err := sqliteagg.StreamFromS3Memory(ctx, client, memCfg)
	if err != nil {
		return fmt.Errorf("stream from S3: %w", err)
	}

	log.Info().
		Int("chunks_processed", result.ChunksProcessed).
		Int64("objects_processed", result.ObjectsProcessed).
		Msg("streaming aggregation complete")

	// Build index from SQLite
	sqliteCfg := sqliteagg.DefaultConfig(dbPath)
	buildCfg := indexbuild.SQLiteConfig{
		OutDir:    outDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteCfg,
	}

	if err := indexbuild.BuildFromSQLite(buildCfg); err != nil {
		return fmt.Errorf("build index from SQLite: %w", err)
	}

	// Get the prefix count for final log
	agg, err := sqliteagg.Open(sqliteCfg)
	if err == nil {
		prefixCount, _ := agg.PrefixCount()
		agg.Close()
		log.Info().
			Str("phase", "build_complete").
			Str("output_dir", outDir).
			Uint64("prefix_count", prefixCount).
			Msg("index built successfully")
	} else {
		log.Info().
			Str("phase", "build_complete").
			Str("output_dir", outDir).
			Msg("index built successfully")
	}

	return nil
}

// runBuildExtSort runs the build using the external sort backend (pure Go, no CGO).
func runBuildExtSort(log *zerolog.Logger, outDir, s3Manifest string) error {
	log.Info().
		Str("phase", "build_start").
		Str("backend", "extsort").
		Str("s3_manifest", s3Manifest).
		Str("output_dir", outDir).
		Msg("starting S3 inventory build with external sort backend")

	// TODO: Implement external sort backend
	return errors.New("extsort backend not yet implemented")
}

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	indexDir := fs.String("index", "", "index directory to query")
	prefix := fs.String("prefix", "", "prefix to query")
	showTiers := fs.Bool("show-tiers", false, "show per-tier breakdown")
	estimateCost := fs.Bool("estimate-cost", false, "estimate monthly storage cost")
	priceTablePath := fs.String("price-table", "", "path to price table JSON (default: US East 1 prices)")
	verbose := fs.Bool("verbose", false, "enable debug level logging")
	prettyLogs := fs.Bool("pretty-logs", false, "use human-friendly console output")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	// Initialize logging based on flags
	logging.Init(*verbose, *prettyLogs)
	log := logging.L()

	if *indexDir == "" {
		return errors.New("--index is required")
	}
	if *prefix == "" {
		return errors.New("--prefix is required")
	}

	log.Debug().Str("index_dir", *indexDir).Str("prefix", *prefix).Msg("opening index")

	idx, err := indexread.Open(*indexDir)
	if err != nil {
		return fmt.Errorf("open index: %w", err)
	}
	defer idx.Close()

	pos, ok := idx.Lookup(*prefix)
	if !ok {
		return fmt.Errorf("prefix not found: %s", *prefix)
	}

	stats := idx.Stats(pos)
	// Query results go to stdout as formatted output (not logs)
	fmt.Printf("Prefix: %s\n", *prefix)
	fmt.Printf("Objects: %d\n", stats.ObjectCount)
	fmt.Printf("Bytes: %d\n", stats.TotalBytes)

	if *showTiers || *estimateCost {
		if !idx.HasTierData() {
			fmt.Println("\nNo tier data available (index was built without tier tracking)")
		} else {
			breakdown := idx.TierBreakdown(pos)
			if len(breakdown) == 0 {
				fmt.Println("\nNo tier data at this prefix")
			} else {
				if *showTiers {
					fmt.Println("\nTier breakdown:")
					for _, tb := range breakdown {
						fmt.Printf("  %s: %d objects, %d bytes\n", tb.TierName, tb.ObjectCount, tb.Bytes)
					}
				}

				if *estimateCost {
					var pt pricing.PriceTable
					if *priceTablePath != "" {
						pt, err = pricing.LoadPriceTable(*priceTablePath)
						if err != nil {
							return fmt.Errorf("load price table: %w", err)
						}
					} else {
						pt = pricing.DefaultUSEast1Prices()
					}

					cost := pricing.ComputeMonthlyCost(breakdown, pt)
					fmt.Println("\nEstimated monthly cost:")
					fmt.Printf("  Total: %s/month\n", pricing.FormatCost(cost.TotalMicrodollars))
					if *showTiers {
						// Sort tier names for deterministic output
						tierNames := make([]string, 0, len(cost.PerTierMicrodollars))
						for tier := range cost.PerTierMicrodollars {
							tierNames = append(tierNames, tier)
						}
						sort.Strings(tierNames)
						for _, tier := range tierNames {
							fmt.Printf("  %s: %s/month\n", tier, pricing.FormatCost(cost.PerTierMicrodollars[tier]))
						}
					}
				}
			}
		}
	}

	return nil
}
