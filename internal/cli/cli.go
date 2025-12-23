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

	"github.com/eunmann/s3-inv-db/internal/logctx"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/membudget"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
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

	// Initialize logging with context-based logger
	baseLogger := logctx.NewConfiguredLogger(*verbose, *prettyLogs)
	logctx.SetDefaultLogger(baseLogger)
	logging.Init(*verbose, *prettyLogs) // Keep legacy logging for existing code

	if *outDir == "" {
		return errors.New("--out is required")
	}
	if *s3Manifest == "" {
		return errors.New("--s3-manifest is required")
	}

	return runBuildExtSort(*outDir, *s3Manifest, *workers, *maxDepth, *memBudgetStr, baseLogger)
}

// runBuildExtSort runs the build using the external sort backend (pure Go, no CGO).
func runBuildExtSort(outDir, s3Manifest string, workers, maxDepth int, memBudgetStr string, baseLogger zerolog.Logger) error {
	// Create a context that responds to OS signals (SIGINT, SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Inject the logger into context for pipeline functions
	ctx = logctx.WithLogger(ctx, baseLogger)
	log := logctx.FromContext(ctx)

	// Determine memory budget
	budget, err := determineMemoryBudget(memBudgetStr)
	if err != nil {
		return fmt.Errorf("invalid memory budget: %w", err)
	}

	// Log memory budget at startup
	ramResult := sysmem.Total()
	log.Info().
		Str("total_ram", humanfmt.BytesUint64(ramResult.TotalBytes)).
		Str("mem_budget", humanfmt.BytesUint64(budget.Total())).
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
// or auto-detection in this order of priority:
//  1. CLI flag --mem-budget
//  2. Environment variable S3INV_MEM_BUDGET
//  3. 50% of detected system RAM.
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

	if !*showTiers && !*estimateCost {
		return nil
	}

	return printTierAndCostInfo(idx, pos, *showTiers, *estimateCost, *priceTablePath)
}

// printTierAndCostInfo handles tier breakdown and cost estimation output.
func printTierAndCostInfo(idx *indexread.Index, pos uint64, showTiers, estimateCost bool, priceTablePath string) error {
	if !idx.HasTierData() {
		fmt.Println("\nNo tier data available (index was built without tier tracking)")
		return nil
	}

	breakdown := idx.TierBreakdown(pos)
	if len(breakdown) == 0 {
		fmt.Println("\nNo tier data at this prefix")
		return nil
	}

	if showTiers {
		printTierBreakdown(breakdown)
	}

	if estimateCost {
		return printCostEstimate(breakdown, showTiers, priceTablePath)
	}

	return nil
}

// printTierBreakdown outputs the tier breakdown to stdout.
func printTierBreakdown(breakdown []format.TierBreakdown) {
	fmt.Println("\nTier breakdown:")
	for _, tb := range breakdown {
		fmt.Printf("  %s: %d objects, %d bytes\n", tb.TierName, tb.ObjectCount, tb.Bytes)
	}
}

// printCostEstimate outputs the cost estimation to stdout.
func printCostEstimate(breakdown []format.TierBreakdown, showTiers bool, priceTablePath string) error {
	pt, err := loadPriceTable(priceTablePath)
	if err != nil {
		return err
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)
	fmt.Println("\nEstimated monthly cost:")
	fmt.Printf("  Total: %s/month\n", pricing.FormatCost(cost.TotalMicrodollars))

	if showTiers {
		printPerTierCosts(cost.PerTierMicrodollars)
	}

	return nil
}

// loadPriceTable loads a price table from a file or returns the default.
func loadPriceTable(path string) (pricing.PriceTable, error) {
	if path != "" {
		pt, err := pricing.LoadPriceTable(path)
		if err != nil {
			return pricing.PriceTable{}, fmt.Errorf("load price table: %w", err)
		}
		return pt, nil
	}
	return pricing.DefaultUSEast1Prices(), nil
}

// printPerTierCosts outputs per-tier cost breakdown in sorted order.
func printPerTierCosts(perTierMicrodollars map[string]uint64) {
	tierNames := make([]string, 0, len(perTierMicrodollars))
	for tier := range perTierMicrodollars {
		tierNames = append(tierNames, tier)
	}
	sort.Strings(tierNames)
	for _, tier := range tierNames {
		fmt.Printf("  %s: %s/month\n", tier, pricing.FormatCost(perTierMicrodollars[tier]))
	}
}
