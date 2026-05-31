package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)


type queryOutput struct {
	Prefix        string                 `json:"prefix"`
	ObjectCount   uint64                 `json:"object_count"`
	TotalBytes    uint64                 `json:"total_bytes"`
	TierBreakdown []format.TierBreakdown `json:"tier_breakdown,omitempty"`
	CostEstimate  *queryCostEstimate     `json:"cost_estimate,omitempty"`
}

type queryCostEstimate struct {
	TotalMicrodollars   uint64            `json:"total_microdollars"`
	PerTierMicrodollars map[string]uint64 `json:"per_tier_microdollars,omitempty"`
}

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to JSON config file (overridden by explicit flags)")
	indexDir := fs.String("index", "", "index directory to query")
	prefix := fs.String("prefix", "", "prefix to query")
	showTiers := fs.Bool("show-tiers", false, "show per-tier breakdown")
	estimateCost := fs.Bool("estimate-cost", false, "estimate monthly storage cost")
	priceTablePath := fs.String("price-table", "", "path to price table JSON (default: US East 1 prices)")
	logFlags := addLoggingFlags(fs)
	outputFlag := addOutputFlag(fs)

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	out, err := parseOutputFormat(*outputFlag)
	if err != nil {
		return err
	}

	fileCfg, err := appconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	initLogging(logFlags, fs, fileCfg)
	finalPriceTable := resolveString(fileCfg, *priceTablePath, explicitFlags(fs)["price-table"], func(c *appconfig.Config) *string { return c.PriceTable })

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
	result := queryOutput{
		Prefix:      *prefix,
		ObjectCount: stats.ObjectCount,
		TotalBytes:  stats.TotalBytes,
	}

	if (*showTiers || *estimateCost) && idx.HasTierData() {
		breakdown := idx.TierBreakdown(pos)
		if *showTiers {
			result.TierBreakdown = breakdown
		}
		if *estimateCost && len(breakdown) > 0 {
			pt, err := LoadPriceTable(finalPriceTable)
			if err != nil {
				return err
			}
			cost := pricing.ComputeMonthlyCost(breakdown, pt)
			result.CostEstimate = &queryCostEstimate{
				TotalMicrodollars:   cost.TotalMicrodollars,
				PerTierMicrodollars: cost.PerTierMicrodollars,
			}
		}
	}

	if out == OutputJSON {
		return writeJSON(os.Stdout, result)
	}

	printQueryText(result, *showTiers, *estimateCost, idx.HasTierData())

	return nil
}

func printQueryText(r queryOutput, showTiers, estimateCost, hasTierData bool) {
	fmt.Fprintf(os.Stdout, "Prefix: %s\n", r.Prefix)
	fmt.Fprintf(os.Stdout, "Objects: %d\n", r.ObjectCount)
	fmt.Fprintf(os.Stdout, "Bytes: %d\n", r.TotalBytes)

	if !showTiers && !estimateCost {
		return
	}

	if !hasTierData {
		fmt.Fprintln(os.Stdout, "\nNo tier data available (index was built without tier tracking)")

		return
	}

	if len(r.TierBreakdown) == 0 && r.CostEstimate == nil {
		fmt.Fprintln(os.Stdout, "\nNo tier data at this prefix")

		return
	}

	if showTiers && len(r.TierBreakdown) > 0 {
		fmt.Fprintln(os.Stdout, "\nTier breakdown:")
		for _, tb := range r.TierBreakdown {
			fmt.Fprintf(os.Stdout, "  %s: %d objects, %d bytes\n", tb.TierName, tb.ObjectCount, tb.Bytes)
		}
	}

	if r.CostEstimate != nil {
		fmt.Fprintln(os.Stdout, "\nEstimated monthly cost:")
		fmt.Fprintf(os.Stdout, "  Total: %s/month\n", pricing.FormatCost(r.CostEstimate.TotalMicrodollars))
		if showTiers {
			printPerTierCosts(r.CostEstimate.PerTierMicrodollars)
		}
	}
}

// LoadPriceTable loads a price table from disk or returns the default.
//
// Exported for cli_test.go.
func LoadPriceTable(path string) (pricing.PriceTable, error) {
	if path != "" {
		pt, err := pricing.LoadPriceTable(path)
		if err != nil {
			return pricing.PriceTable{}, fmt.Errorf("load price table: %w", err)
		}

		return pt, nil
	}

	return pricing.DefaultUSEast1Prices(), nil
}

func printPerTierCosts(perTierMicrodollars map[string]uint64) {
	tierNames := make([]string, 0, len(perTierMicrodollars))
	for tier := range perTierMicrodollars {
		tierNames = append(tierNames, tier)
	}
	slices.Sort(tierNames)
	for _, tier := range tierNames {
		fmt.Fprintf(os.Stdout, "  %s: %s/month\n", tier, pricing.FormatCost(perTierMicrodollars[tier]))
	}
}

func writeJSON(w *os.File, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}

	return nil
}
