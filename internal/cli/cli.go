// Package cli implements the command-line interface for s3inv-index.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/indexbuild"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

// Run executes the CLI with the given arguments.
func Run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: s3inv-index <command> [options]\ncommands: build, query")
	}

	switch args[0] {
	case "build":
		return runBuild(args[1:])
	case "query":
		return runQuery(args[1:])
	default:
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func runBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	outDir := fs.String("out", "", "output directory for index files")
	tmpDir := fs.String("tmp", "", "temporary directory for sort runs")
	chunkSize := fs.Int("chunk-size", 1_000_000, "max records per sort chunk")
	trackTiers := fs.Bool("track-tiers", false, "enable per-tier byte and count tracking")
	s3Manifest := fs.String("s3-manifest", "", "S3 URI to inventory manifest.json (s3://bucket/path/manifest.json)")
	downloadConcurrency := fs.Int("download-concurrency", 4, "number of parallel S3 downloads")
	keepDownloads := fs.Bool("keep-downloads", false, "keep downloaded inventory files after building")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *outDir == "" {
		return errors.New("--out is required")
	}
	if *tmpDir == "" {
		return errors.New("--tmp is required")
	}

	// Check if building from S3 manifest
	if *s3Manifest != "" {
		return runBuildFromS3(*outDir, *tmpDir, *chunkSize, *trackTiers, *s3Manifest, *downloadConcurrency, *keepDownloads)
	}

	// Building from local files
	inventoryFiles := fs.Args()
	if len(inventoryFiles) == 0 {
		return errors.New("at least one inventory file is required (or use --s3-manifest)")
	}

	// Check if any local file looks like an S3 URI
	for _, f := range inventoryFiles {
		if strings.HasPrefix(f, "s3://") {
			return fmt.Errorf("S3 URIs should be passed via --s3-manifest, not as positional arguments: %s", f)
		}
	}

	cfg := indexbuild.Config{
		OutDir:     *outDir,
		TmpDir:     *tmpDir,
		ChunkSize:  *chunkSize,
		TrackTiers: *trackTiers,
	}

	if err := indexbuild.Build(context.Background(), cfg, inventoryFiles); err != nil {
		return fmt.Errorf("build failed: %w", err)
	}

	fmt.Printf("Index built successfully: %s\n", *outDir)
	if *trackTiers {
		fmt.Println("Tier statistics enabled.")
	}
	return nil
}

func runBuildFromS3(outDir, tmpDir string, chunkSize int, trackTiers bool, manifestURI string, concurrency int, keepDownloads bool) error {
	fmt.Printf("Fetching inventory from S3: %s\n", manifestURI)

	cfg := indexbuild.S3Config{
		Config: indexbuild.Config{
			OutDir:     outDir,
			TmpDir:     tmpDir,
			ChunkSize:  chunkSize,
			TrackTiers: trackTiers,
		},
		ManifestURI:         manifestURI,
		DownloadConcurrency: concurrency,
		KeepDownloads:       keepDownloads,
	}

	if err := indexbuild.BuildFromS3(context.Background(), cfg); err != nil {
		return fmt.Errorf("build from S3 failed: %w", err)
	}

	fmt.Printf("Index built successfully: %s\n", outDir)
	if trackTiers {
		fmt.Println("Tier statistics enabled.")
	}
	return nil
}

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	indexDir := fs.String("index", "", "index directory to query")
	prefix := fs.String("prefix", "", "prefix to query")
	showTiers := fs.Bool("show-tiers", false, "show per-tier breakdown")
	estimateCost := fs.Bool("estimate-cost", false, "estimate monthly storage cost")
	priceTablePath := fs.String("price-table", "", "path to price table JSON (default: US East 1 prices)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *indexDir == "" {
		return errors.New("--index is required")
	}
	if *prefix == "" {
		return errors.New("--prefix is required")
	}

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
	fmt.Printf("Prefix: %s\n", *prefix)
	fmt.Printf("Objects: %d\n", stats.ObjectCount)
	fmt.Printf("Bytes: %d\n", stats.TotalBytes)

	if *showTiers || *estimateCost {
		if !idx.HasTierData() {
			fmt.Println("\nNo tier data available (index was built without --track-tiers)")
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
						for tier, microdollars := range cost.PerTierMicrodollars {
							fmt.Printf("  %s: %s/month\n", tier, pricing.FormatCost(microdollars))
						}
					}
				}
			}
		}
	}

	return nil
}
