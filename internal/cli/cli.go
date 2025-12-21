// Package cli implements the command-line interface for s3inv-index.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/membudget"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
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
	verbose := fs.Bool("verbose", false, "enable debug level logging")
	prettyLogs := fs.Bool("pretty-logs", false, "use human-friendly console output")

	// Concurrency tuning
	workers := fs.Int("workers", 0, "number of concurrent S3 download/parse workers (default: CPU count)")
	maxDepth := fs.Int("max-depth", 0, "maximum prefix depth to track (0 = unlimited)")

	// Memory budget
	memBudgetStr := fs.String("mem-budget", "", "total memory budget (e.g., 4GiB, 8GB). Default: 50% of RAM")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	logging.Init(*verbose, *prettyLogs)

	if *outDir == "" {
		return errors.New("--out is required")
	}
	if *s3Manifest == "" {
		return errors.New("--s3-manifest is required")
	}

	return runBuildExtSort(*outDir, *s3Manifest, *workers, *maxDepth, *memBudgetStr)
}

// runBuildExtSort runs the build using the external sort backend (pure Go, no CGO).
func runBuildExtSort(outDir, s3Manifest string, workers, maxDepth int, memBudgetStr string) error {
	log := logging.L()

	// Create a context that responds to OS signals (SIGINT, SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Determine memory budget
	budget, err := determineMemoryBudget(memBudgetStr)
	if err != nil {
		return fmt.Errorf("invalid memory budget: %w", err)
	}

	// Log memory budget at startup
	ramResult := sysmem.Total()
	log.Info().
		Str("total_ram", membudget.FormatBytes(ramResult.TotalBytes)).
		Str("mem_budget", membudget.FormatBytes(budget.Total())).
		Str("mem_budget_source", string(budget.Source())).
		Msg("memory budget configured")

	client, err := s3fetch.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	config := extsort.DefaultConfig()
	config.MemoryBudget = budget

	// Apply CLI overrides
	if workers > 0 {
		config.S3DownloadConcurrency = workers
		config.ParseConcurrency = workers
		config.IndexWriteConcurrency = workers
	}
	if maxDepth > 0 {
		config.MaxDepth = maxDepth
	}

	// Log concurrency settings
	log.Info().
		Int("workers", config.S3DownloadConcurrency).
		Int("max_depth", config.MaxDepth).
		Msg("pipeline configuration")

	pipeline := extsort.NewPipeline(config, client)

	_, err = pipeline.Run(ctx, s3Manifest, outDir)
	if err != nil {
		// Check if this was a cancellation
		if errors.Is(err, context.Canceled) {
			log.Warn().Msg("build cancelled by user")
			return fmt.Errorf("build cancelled: %w", err)
		}
		return fmt.Errorf("run pipeline: %w", err)
	}

	return nil
}

// determineMemoryBudget determines the memory budget from CLI flag, environment variable,
// or system RAM detection.
//
// Priority order:
// 1. CLI flag (--mem-budget) if provided
// 2. Environment variable (S3INV_MEM_BUDGET) if set
// 3. 50% of detected system RAM
func determineMemoryBudget(cliValue string) (*membudget.Budget, error) {
	// Check CLI flag first
	if cliValue != "" {
		bytes, err := membudget.ParseHumanSize(cliValue)
		if err != nil {
			return nil, fmt.Errorf("parse --mem-budget: %w", err)
		}
		return membudget.New(membudget.Config{
			TotalBytes: bytes,
			Source:     membudget.BudgetSourceCLI,
		}), nil
	}

	// Check environment variable
	if envValue := os.Getenv("S3INV_MEM_BUDGET"); envValue != "" {
		bytes, err := membudget.ParseHumanSize(envValue)
		if err != nil {
			return nil, fmt.Errorf("parse S3INV_MEM_BUDGET=%q: %w", envValue, err)
		}
		return membudget.New(membudget.Config{
			TotalBytes: bytes,
			Source:     membudget.BudgetSourceEnv,
		}), nil
	}

	// Fall back to 50% of system RAM
	return membudget.NewFromSystemRAM(), nil
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
