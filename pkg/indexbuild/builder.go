// Package indexbuild constructs S3 inventory indexes from SQLite aggregated data.
package indexbuild

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
	"github.com/eunmann/s3-inv-db/pkg/triebuild"
)

// SQLiteConfig holds configuration for building from a SQLite aggregator.
type SQLiteConfig struct {
	// OutDir is the output directory for index files.
	OutDir string
	// DBPath is the path to the SQLite prefix aggregation database.
	DBPath string
	// SQLiteCfg is the SQLite configuration.
	SQLiteCfg sqliteagg.Config
}

// BuildFromSQLite builds an index from a pre-populated SQLite prefix aggregation database.
// The database must have been populated using sqliteagg.StreamFromS3 or equivalent.
func BuildFromSQLite(cfg SQLiteConfig) error {
	log := logging.L()

	if cfg.OutDir == "" {
		return fmt.Errorf("output directory required")
	}
	if cfg.DBPath == "" {
		return fmt.Errorf("database path required")
	}

	// Check if output directory already has a valid index (resume support)
	if indexValid(cfg.OutDir) {
		log.Info().
			Str("output_dir", cfg.OutDir).
			Msg("resumed from completed build - index already valid")
		return nil
	}

	// Open SQLite aggregator
	agg, err := sqliteagg.Open(cfg.SQLiteCfg)
	if err != nil {
		return fmt.Errorf("open SQLite aggregator: %w", err)
	}
	defer agg.Close()

	// Build trie from SQLite
	result, err := sqliteagg.BuildTrieFromSQLite(agg)
	if err != nil {
		return fmt.Errorf("build trie from SQLite: %w", err)
	}

	if len(result.Nodes) == 0 {
		return fmt.Errorf("no nodes built from SQLite database")
	}

	// Create temp output directory
	tmpOutDir := cfg.OutDir + ".tmp"
	if err := os.MkdirAll(tmpOutDir, 0755); err != nil {
		return fmt.Errorf("create temp output dir: %w", err)
	}

	// Track whether we successfully renamed - only cleanup on failure
	renamed := false
	defer func() {
		if !renamed {
			os.RemoveAll(tmpOutDir)
		}
	}()

	// Write columnar arrays
	if err := writeColumnarArrays(tmpOutDir, result); err != nil {
		return fmt.Errorf("write columnar arrays: %w", err)
	}

	// Write tier stats (if tracking enabled)
	if result.TrackTiers {
		if err := writeTierStats(tmpOutDir, result); err != nil {
			return fmt.Errorf("write tier stats: %w", err)
		}
	}

	// Build depth index
	if err := writeDepthIndex(tmpOutDir, result); err != nil {
		return fmt.Errorf("write depth index: %w", err)
	}

	// Build MPHF
	if err := writeMPHF(tmpOutDir, result); err != nil {
		return fmt.Errorf("write MPHF: %w", err)
	}

	// Write manifest with checksums
	if err := format.WriteManifest(tmpOutDir, uint64(len(result.Nodes)), result.MaxDepth); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Sync directory to ensure all files are persisted
	if err := format.SyncDir(tmpOutDir); err != nil {
		return fmt.Errorf("sync output dir: %w", err)
	}

	// Atomic move to final location
	if err := os.RemoveAll(cfg.OutDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old output dir: %w", err)
	}
	if err := os.Rename(tmpOutDir, cfg.OutDir); err != nil {
		return fmt.Errorf("rename output dir: %w", err)
	}
	renamed = true

	// Best-effort sync of parent directory to persist the rename
	parentDir := filepath.Dir(cfg.OutDir)
	_ = format.SyncDir(parentDir)

	log.Info().
		Str("output_dir", cfg.OutDir).
		Int("node_count", len(result.Nodes)).
		Msg("index build from SQLite complete")

	return nil
}

func writeColumnarArrays(outDir string, result *triebuild.Result) error {
	log := logging.WithPhase("write_index")
	start := time.Now()

	log.Info().
		Str("output_dir", outDir).
		Int("node_count", len(result.Nodes)).
		Uint32("max_depth", result.MaxDepth).
		Msg("writing columnar arrays")

	// Write u64 arrays
	u64Arrays := []struct {
		name   string
		getter func(triebuild.Node) uint64
	}{
		{"subtree_end.u64", func(n triebuild.Node) uint64 { return n.SubtreeEnd }},
		{"object_count.u64", func(n triebuild.Node) uint64 { return n.ObjectCount }},
		{"total_bytes.u64", func(n triebuild.Node) uint64 { return n.TotalBytes }},
	}

	for _, arr := range u64Arrays {
		if err := writeU64Array(outDir, arr.name, result.Nodes, arr.getter); err != nil {
			return fmt.Errorf("write %s: %w", arr.name, err)
		}
		log.Debug().Str("file", arr.name).Msg("wrote array file")
	}

	// Write u32 arrays
	u32Arrays := []struct {
		name   string
		getter func(triebuild.Node) uint32
	}{
		{"depth.u32", func(n triebuild.Node) uint32 { return n.Depth }},
		{"max_depth_in_subtree.u32", func(n triebuild.Node) uint32 { return n.MaxDepthInSubtree }},
	}

	for _, arr := range u32Arrays {
		if err := writeU32Array(outDir, arr.name, result.Nodes, arr.getter); err != nil {
			return fmt.Errorf("write %s: %w", arr.name, err)
		}
		log.Debug().Str("file", arr.name).Msg("wrote array file")
	}

	log.Info().
		Int("files_written", len(u64Arrays)+len(u32Arrays)).
		Dur("elapsed", time.Since(start)).
		Msg("columnar arrays written")

	return nil
}

func writeU64Array(outDir, name string, nodes []triebuild.Node, getter func(triebuild.Node) uint64) error {
	path := filepath.Join(outDir, name)
	writer, err := format.NewArrayWriter(path, 8)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}
	for i, node := range nodes {
		if err := writer.WriteU64(getter(node)); err != nil {
			writer.Close()
			return fmt.Errorf("write node %d: %w", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	return nil
}

func writeU32Array(outDir, name string, nodes []triebuild.Node, getter func(triebuild.Node) uint32) error {
	path := filepath.Join(outDir, name)
	writer, err := format.NewArrayWriter(path, 4)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}
	for i, node := range nodes {
		if err := writer.WriteU32(getter(node)); err != nil {
			writer.Close()
			return fmt.Errorf("write node %d: %w", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	return nil
}

func writeDepthIndex(outDir string, result *triebuild.Result) error {
	log := logging.WithPhase("write_index")
	start := time.Now()

	log.Info().Int("node_count", len(result.Nodes)).Msg("building depth index")

	builder := format.NewDepthIndexBuilder()

	for _, node := range result.Nodes {
		builder.Add(node.Pos, node.Depth)
	}

	if err := builder.Build(outDir); err != nil {
		return fmt.Errorf("build depth index: %w", err)
	}

	log.Info().Dur("elapsed", time.Since(start)).Msg("depth index complete")
	return nil
}

func writeMPHF(outDir string, result *triebuild.Result) error {
	log := logging.WithPhase("build_mphf")
	start := time.Now()

	log.Info().Int("key_count", len(result.Nodes)).Msg("building MPHF")

	builder := format.NewMPHFBuilder()

	for _, node := range result.Nodes {
		builder.Add(node.Prefix, node.Pos)
	}

	if err := builder.Build(outDir); err != nil {
		return fmt.Errorf("build MPHF: %w", err)
	}

	log.Info().
		Int("key_count", builder.Count()).
		Dur("elapsed", time.Since(start)).
		Msg("MPHF build complete")
	return nil
}

func writeTierStats(outDir string, result *triebuild.Result) error {
	log := logging.WithPhase("tiers_build")
	start := time.Now()

	if !result.TrackTiers || len(result.PresentTiers) == 0 {
		log.Info().Msg("no tier data to write")
		return nil
	}

	log.Info().
		Int("tier_count", len(result.PresentTiers)).
		Msg("writing tier statistics")

	tierWriter, err := format.NewTierStatsWriter(outDir)
	if err != nil {
		return fmt.Errorf("create tier stats writer: %w", err)
	}
	if err := tierWriter.Write(result); err != nil {
		return err
	}

	log.Info().
		Int("tiers_written", len(result.PresentTiers)).
		Dur("elapsed", time.Since(start)).
		Msg("tier statistics complete")

	return nil
}

// indexValid checks if a complete index exists and is valid by reading the manifest
// and verifying all files match their checksums.
func indexValid(outDir string) bool {
	manifest, err := format.ReadManifest(outDir)
	if err != nil {
		return false
	}

	if err := format.VerifyManifest(outDir, manifest); err != nil {
		return false
	}

	return true
}
