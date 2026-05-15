// Package cli implements the command-line interface for s3-inv-db.
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

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Sentinel errors keep `err113` happy and let callers test for the
// specific failure mode with errors.Is.
var (
	ErrUsage           = errors.New("usage: s3-inv-db <command> [options]\ncommands: build, query")
	ErrUnknownCommand  = errors.New("unknown command")
	ErrOutRequired     = errors.New("--out is required")
	ErrManifestRequire = errors.New("--s3-manifest is required")
	ErrIndexRequired   = errors.New("--index is required")
	ErrPrefixRequired  = errors.New("--prefix is required")
	ErrPrefixNotFound  = errors.New("prefix not found")
)

// Run executes the CLI with the given arguments.
func Run(args []string) error {
	if len(args) == 0 {
		return ErrUsage
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "build":
		return runBuild(cmdArgs)
	case "query":
		return runQuery(cmdArgs)
	default:
		return fmt.Errorf("%w: %s", ErrUnknownCommand, cmd)
	}
}

func runBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	configPath := fs.String("config", os.Getenv("S3INV_CONFIG"), "path to JSON config file (overridden by explicit flags)")
	outDir := fs.String("out", "", "output directory for index files")
	s3Manifest := fs.String("s3-manifest", "", "S3 URI to inventory manifest.json (s3://bucket/path/manifest.json)")
	verbose := fs.Bool("verbose", false, "enable debug level logging")
	prettyLogs := fs.Bool("pretty-logs", false, "use human-friendly console output")

	// Concurrency tuning
	maxDepth := fs.Int("max-depth", 0, "maximum prefix depth to track (0 = unlimited)")

	// Prefix encoding
	segmentPrefixes := fs.Bool("segment-prefixes", false, "use segment dictionary compression for prefixes (reduces size when prefixes share path components)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	fileCfg, err := appconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	explicit := explicitFlags(fs)
	finalVerbose := resolveBool(fileCfg, *verbose, explicit["verbose"], func(c *appconfig.Config) *bool { return c.Verbose })
	finalPretty := resolveBool(fileCfg, *prettyLogs, explicit["pretty-logs"], func(c *appconfig.Config) *bool { return c.PrettyLogs })

	baseLogger := logging.NewLogger(finalVerbose, finalPretty)
	log.Logger = baseLogger
	logging.Init(finalVerbose, finalPretty)

	memLimit := sysmem.ApplyMemoryLimit(sysmem.DefaultMemoryLimitFraction)
	baseLogger.Info().
		Int64("bytes", memLimit.Bytes).
		Str("source", string(memLimit.Source)).
		Int64("env_bytes", memLimit.EnvBytes).
		Int64("cgroup_bytes", memLimit.CgroupBytes).
		Int64("sysmem_fraction_bytes", memLimit.SysmemFractionBytes).
		Msg("process memory limit configured")

	if *outDir == "" {
		return ErrOutRequired
	}
	if *s3Manifest == "" {
		return ErrManifestRequire
	}

	return runBuildExtSort(*outDir, *s3Manifest, *maxDepth, *segmentPrefixes, baseLogger)
}

// runBuildExtSort runs the build using the external sort backend (pure Go, no CGO).
func runBuildExtSort(outDir, s3Manifest string, maxDepth int, segmentPrefixes bool, baseLogger zerolog.Logger) error {
	// Create a context that responds to OS signals (SIGINT, SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Inject the logger into ctx for pipeline functions. zerolog's
	// WithContext returns a new ctx; zerolog.Ctx(ctx) retrieves later.
	ctx = baseLogger.WithContext(ctx)
	logger := zerolog.Ctx(ctx)

	client, err := s3fetch.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	config := extsort.DefaultConfig()
	if maxDepth > 0 {
		config.MaxDepth = maxDepth
	}
	config.UseSegmentEncoding = segmentPrefixes

	logger.Info().
		Int("parse_workers", config.ParseConcurrency).
		Int("index_write_workers", config.IndexWriteConcurrency).
		Int("s3_part_concurrency", config.S3DownloadPartConcurrency).
		Int("max_depth", config.MaxDepth).
		Msg("pipeline configuration")

	pipeline := extsort.NewPipeline(config, client)

	_, err = pipeline.Run(ctx, s3Manifest, outDir)
	if err != nil {
		// Check if this was a cancellation
		if errors.Is(err, context.Canceled) {
			logger.Warn().Msg("build cancelled by user")

			return fmt.Errorf("build cancelled: %w", err)
		}

		return fmt.Errorf("run pipeline: %w", err)
	}

	return nil
}

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	configPath := fs.String("config", os.Getenv("S3INV_CONFIG"), "path to JSON config file (overridden by explicit flags)")
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

	fileCfg, err := appconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	explicit := explicitFlags(fs)
	finalVerbose := resolveBool(fileCfg, *verbose, explicit["verbose"], func(c *appconfig.Config) *bool { return c.Verbose })
	finalPretty := resolveBool(fileCfg, *prettyLogs, explicit["pretty-logs"], func(c *appconfig.Config) *bool { return c.PrettyLogs })
	finalPriceTable := resolveString(fileCfg, *priceTablePath, explicit["price-table"], func(c *appconfig.Config) *string { return c.PriceTable })

	logging.Init(finalVerbose, finalPretty)
	logger := logging.L()

	if *indexDir == "" {
		return ErrIndexRequired
	}
	if *prefix == "" {
		return ErrPrefixRequired
	}

	logger.Debug().Str("index_dir", *indexDir).Str("prefix", *prefix).Msg("opening index")

	idx, err := indexread.Open(*indexDir)
	if err != nil {
		return fmt.Errorf("open index: %w", err)
	}
	defer idx.Close()

	pos, ok := idx.Lookup(*prefix)
	if !ok {
		return fmt.Errorf("%w: %s", ErrPrefixNotFound, *prefix)
	}

	stats := idx.Stats(pos)
	// Query results go to stdout as formatted output (not logs).
	fmt.Fprintf(os.Stdout, "Prefix: %s\n", *prefix)
	fmt.Fprintf(os.Stdout, "Objects: %d\n", stats.ObjectCount)
	fmt.Fprintf(os.Stdout, "Bytes: %d\n", stats.TotalBytes)

	if !*showTiers && !*estimateCost {
		return nil
	}

	return printTierAndCostInfo(idx, pos, *showTiers, *estimateCost, finalPriceTable)
}

func explicitFlags(fs *flag.FlagSet) map[string]bool {
	out := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { out[f.Name] = true })

	return out
}

func resolveBool(cfg *appconfig.Config, flagVal, explicit bool, get func(*appconfig.Config) *bool) bool {
	var p *bool
	if cfg != nil {
		p = get(cfg)
	}

	return appconfig.PickBool(flagVal, explicit, p)
}

func resolveString(cfg *appconfig.Config, flagVal string, explicit bool, get func(*appconfig.Config) *string) string {
	var p *string
	if cfg != nil {
		p = get(cfg)
	}

	return appconfig.PickString(flagVal, explicit, p)
}

// printTierAndCostInfo handles tier breakdown and cost estimation output.
func printTierAndCostInfo(idx *indexread.Index, pos uint64, showTiers, estimateCost bool, priceTablePath string) error {
	if !idx.HasTierData() {
		fmt.Fprintln(os.Stdout, "\nNo tier data available (index was built without tier tracking)")

		return nil
	}

	breakdown := idx.TierBreakdown(pos)
	if len(breakdown) == 0 {
		fmt.Fprintln(os.Stdout, "\nNo tier data at this prefix")

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
	fmt.Fprintln(os.Stdout, "\nTier breakdown:")
	for _, tb := range breakdown {
		fmt.Fprintf(os.Stdout, "  %s: %d objects, %d bytes\n", tb.TierName, tb.ObjectCount, tb.Bytes)
	}
}

// printCostEstimate outputs the cost estimation to stdout.
func printCostEstimate(breakdown []format.TierBreakdown, showTiers bool, priceTablePath string) error {
	pt, err := loadPriceTable(priceTablePath)
	if err != nil {
		return err
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)
	fmt.Fprintln(os.Stdout, "\nEstimated monthly cost:")
	fmt.Fprintf(os.Stdout, "  Total: %s/month\n", pricing.FormatCost(cost.TotalMicrodollars))

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
		fmt.Fprintf(os.Stdout, "  %s: %s/month\n", tier, pricing.FormatCost(perTierMicrodollars[tier]))
	}
}
