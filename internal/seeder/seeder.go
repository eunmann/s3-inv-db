// Package seeder generates synthetic S3 inventory index directories for local development.
package seeder

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/rs/zerolog"
)

// Target selects the seeder output sink.
type Target string

const (
	// TargetLocal writes built indexes to a local directory.
	TargetLocal Target = "local"
	// TargetS3 uploads simulated S3 Inventory layouts (manifest + CSV.gz) to S3/MinIO.
	TargetS3 Target = "s3"
)

// Config configures the seeder.
type Config struct {
	Target    Target
	OutputDir string // TargetLocal: where indexes are written
	S3        S3Config
	Count     int
	Objects   int
	Preset    string
	Seed      int64
	Logger    zerolog.Logger
}

// InventoryInfo describes a generated inventory.
type InventoryInfo struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Path     string `json:"path"`
	Objects  int    `json:"objects"`
	Prefixes int    `json:"prefixes"`
}

// Summary contains metadata about all generated inventories.
type Summary struct {
	GeneratedAt time.Time       `json:"generated_at"`
	Duration    string          `json:"duration"`
	Inventories []InventoryInfo `json:"inventories"`
}

// Run executes the seeder, generating all inventories. Target selects
// between writing pre-built indexes locally and uploading synthetic AWS S3
// Inventory layouts (manifest + CSV.gz) to S3/MinIO.
func Run(cfg Config) error {
	startTime := time.Now()

	if cfg.Target == "" {
		cfg.Target = TargetLocal
	}

	logEv := cfg.Logger.Info().
		Str("target", string(cfg.Target)).
		Int("count", cfg.Count).
		Int("objects", cfg.Objects).
		Str("preset", cfg.Preset)
	switch cfg.Target {
	case TargetLocal:
		logEv = logEv.Str("output_dir", cfg.OutputDir)
	case TargetS3:
		logEv = logEv.Str("s3_bucket", cfg.S3.Bucket).Str("s3_prefix", cfg.S3.Prefix).Str("s3_src_bucket", cfg.S3.SrcBucket)
	default:
		return fmt.Errorf("unknown seeder target: %q", cfg.Target)
	}
	logEv.Msg("starting seeder")

	switch cfg.Target {
	case TargetLocal:
		return runLocal(cfg, startTime)
	case TargetS3:
		return runS3(cfg, startTime)
	default:
		return fmt.Errorf("unknown seeder target: %q", cfg.Target)
	}
}

func runLocal(cfg Config, startTime time.Time) error {
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	inventories := make([]InventoryInfo, 0, cfg.Count)
	for i := range cfg.Count {
		// Always offset per inventory so distinct inventories get distinct
		// data, including when the user didn't pass --seed (cfg.Seed == 0).
		// Adding 1 keeps invSeed non-zero so the generator never falls back
		// to its built-in default seed.
		invSeed := cfg.Seed + int64(i+1)*1000

		info, err := generateInventory(cfg, i+1, invSeed)
		if err != nil {
			return fmt.Errorf("generate inventory %d: %w", i+1, err)
		}
		inventories = append(inventories, info)

		cfg.Logger.Info().
			Str("id", info.ID).
			Int("objects", info.Objects).
			Int("prefixes", info.Prefixes).
			Msg("generated inventory")
	}

	if err := writeSummary(cfg.OutputDir, inventories, startTime); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}

	cfg.Logger.Info().
		Int("total_inventories", len(inventories)).
		Str("duration", time.Since(startTime).Round(time.Millisecond).String()).
		Msg("seeding complete")

	return nil
}

func runS3(cfg Config, startTime time.Time) error {
	if err := cfg.S3.Validate(); err != nil {
		return fmt.Errorf("invalid s3 config: %w", err)
	}

	ctx := context.Background()
	client, err := newS3Client(ctx)
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}

	// Use one run timestamp for all inventories in this seeding so the
	// folder names line up nicely in tooling.
	runStamp := time.Now().UTC().Truncate(time.Minute)

	for i := range cfg.Count {
		invSeed := cfg.Seed + int64(i+1)*1000
		info, err := UploadInventory(ctx, client, cfg, cfg.S3, i+1, invSeed, runStamp)
		if err != nil {
			return fmt.Errorf("upload inventory %d: %w", i+1, err)
		}
		cfg.Logger.Info().
			Str("id", info.ID).
			Str("manifest", info.Path).
			Int("objects", info.Objects).
			Msg("uploaded inventory")
	}

	cfg.Logger.Info().
		Int("total_inventories", cfg.Count).
		Str("duration", time.Since(startTime).Round(time.Millisecond).String()).
		Msg("seeding complete")

	return nil
}

// generateInventory generates a single inventory index directory.
func generateInventory(cfg Config, index int, seed int64) (InventoryInfo, error) {
	id := fmt.Sprintf("inv-%03d", index)
	name := fmt.Sprintf("Seed Inventory %d", index)
	outDir := filepath.Join(cfg.OutputDir, id)

	genCfg := getGeneratorConfig(cfg.Preset, cfg.Objects)
	if seed != 0 {
		genCfg.Seed = seed
	}

	gen := benchutil.NewGenerator(genCfg)
	objects := gen.Generate()

	agg := extsort.NewAggregator(len(objects), 0)
	for _, obj := range objects {
		agg.AddObject(obj.Key, obj.Size, obj.TierID)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	builder, err := extsort.NewIndexBuilder(outDir, "", false)
	if err != nil {
		return InventoryInfo{}, fmt.Errorf("create index builder: %w", err)
	}

	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			return InventoryInfo{}, fmt.Errorf("add row: %w", err)
		}
	}

	if err := builder.Finalize(); err != nil {
		return InventoryInfo{}, fmt.Errorf("finalize index: %w", err)
	}

	absPath, err := filepath.Abs(outDir)
	if err != nil {
		absPath = outDir
	}

	return InventoryInfo{
		ID:       id,
		Name:     name,
		Path:     absPath,
		Objects:  cfg.Objects,
		Prefixes: int(builder.Count()),
	}, nil
}

// getGeneratorConfig returns a generator configuration for the given preset.
func getGeneratorConfig(preset string, numObjects int) benchutil.GeneratorConfig {
	switch preset {
	case "small":
		cfg := benchutil.DefaultConfig(numObjects)
		cfg.PrefixFanout = 5
		cfg.MaxDepth = 3
		return cfg
	case "medium":
		cfg := benchutil.DefaultConfig(numObjects)
		cfg.PrefixFanout = 10
		cfg.MaxDepth = 5
		return cfg
	case "large":
		cfg := benchutil.DefaultConfig(numObjects)
		cfg.PrefixFanout = 20
		cfg.MaxDepth = 8
		return cfg
	case "realistic":
		return benchutil.S3RealisticConfig(numObjects)
	default:
		return benchutil.S3RealisticConfig(numObjects)
	}
}

// writeSummary writes a summary.json file with information about generated inventories.
func writeSummary(dir string, inventories []InventoryInfo, startTime time.Time) error {
	summary := Summary{
		GeneratedAt: startTime,
		Duration:    time.Since(startTime).Round(time.Millisecond).String(),
		Inventories: inventories,
	}

	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}

	summaryPath := filepath.Join(dir, "summary.json")
	if err := os.WriteFile(summaryPath, data, 0o644); err != nil {
		return fmt.Errorf("write summary file: %w", err)
	}

	return nil
}
