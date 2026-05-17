package seeder_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

func TestRun_GeneratesValidIndexes(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := seeder.Config{
		OutputDir: tmpDir,
		Count:     2,
		Objects:   100,
		Preset:    "small",
		Seed:      42,
		Logger:    zerolog.Nop(),
	}

	if err := seeder.Run(cfg); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify summary.json was created
	summaryPath := filepath.Join(tmpDir, "summary.json")
	summaryData, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary.json: %v", err)
	}

	var summary seeder.Summary
	if err := json.Unmarshal(summaryData, &summary); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}

	if len(summary.Inventories) != 2 {
		t.Errorf("expected 2 inventories, got %d", len(summary.Inventories))
	}

	// Verify each index can be opened
	for _, inv := range summary.Inventories {
		idx, err := indexread.Open(inv.Path)
		if err != nil {
			t.Errorf("failed to open index %s: %v", inv.ID, err)

			continue
		}

		if idx.Count() == 0 {
			t.Errorf("index %s has no prefixes", inv.ID)
		}

		// Verify root prefix exists
		pos, ok := idx.Lookup("")
		if !ok {
			t.Errorf("index %s missing root prefix", inv.ID)
		} else {
			stats := idx.Stats(pos)
			if stats.ObjectCount == 0 {
				t.Errorf("index %s root has no objects", inv.ID)
			}
		}

		idx.Close()
	}
}

func TestRun_DistinctInventoriesWithDefaultSeed(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := seeder.Config{
		OutputDir: tmpDir,
		Count:     3,
		Objects:   200,
		Preset:    "small",
		Seed:      0, // default — must still produce distinct inventories
		Logger:    zerolog.Nop(),
	}

	if err := seeder.Run(cfg); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Compare the raw index payloads — distinct seeds yield distinct trees.
	hashes := make(map[string]string, 3)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("inv-%03d", i)
		blobPath := filepath.Join(tmpDir, id, "prefix_blob.bin")
		data, err := os.ReadFile(blobPath)
		if err != nil {
			t.Fatalf("read %s: %v", blobPath, err)
		}
		h := sha256.Sum256(data)
		hashes[id] = hex.EncodeToString(h[:])
	}

	if hashes["inv-001"] == hashes["inv-002"] {
		t.Errorf("inv-001 and inv-002 produced identical index blobs (seed offset not applied)")
	}
	if hashes["inv-002"] == hashes["inv-003"] {
		t.Errorf("inv-002 and inv-003 produced identical index blobs")
	}
}

func TestGetGeneratorConfig_Presets(t *testing.T) {
	presets := []string{"small", "medium", "large", "realistic", "unknown"}

	for _, preset := range presets {
		t.Run(preset, func(t *testing.T) {
			cfg := seeder.GetGeneratorConfig(preset, 1000)

			if cfg.NumObjects != 1000 {
				t.Errorf("expected 1000 objects, got %d", cfg.NumObjects)
			}

			if cfg.PrefixFanout <= 0 {
				t.Errorf("expected positive prefix fanout, got %d", cfg.PrefixFanout)
			}

			if cfg.MaxDepth <= 0 {
				t.Errorf("expected positive max depth, got %d", cfg.MaxDepth)
			}
		})
	}
}

func TestGenerateInventory_Deterministic(t *testing.T) {
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	cfg1 := seeder.Config{
		OutputDir: tmpDir1,
		Count:     1,
		Objects:   50,
		Preset:    "small",
		Seed:      12345,
		Logger:    zerolog.Nop(),
	}

	cfg2 := seeder.Config{
		OutputDir: tmpDir2,
		Count:     1,
		Objects:   50,
		Preset:    "small",
		Seed:      12345,
		Logger:    zerolog.Nop(),
	}

	info1, err := seeder.GenerateInventory(cfg1, 1, 12345)
	if err != nil {
		t.Fatalf("generate inventory 1: %v", err)
	}

	info2, err := seeder.GenerateInventory(cfg2, 1, 12345)
	if err != nil {
		t.Fatalf("generate inventory 2: %v", err)
	}

	if info1.Objects != info2.Objects {
		t.Errorf("objects mismatch: %d vs %d", info1.Objects, info2.Objects)
	}

	if info1.Prefixes != info2.Prefixes {
		t.Errorf("prefixes mismatch: %d vs %d", info1.Prefixes, info2.Prefixes)
	}

	// Open both and compare stats at root
	idx1, err := indexread.Open(info1.Path)
	if err != nil {
		t.Fatalf("open index 1: %v", err)
	}
	defer idx1.Close()

	idx2, err := indexread.Open(info2.Path)
	if err != nil {
		t.Fatalf("open index 2: %v", err)
	}
	defer idx2.Close()

	pos1, _ := idx1.Lookup("")
	pos2, _ := idx2.Lookup("")

	stats1 := idx1.Stats(pos1)
	stats2 := idx2.Stats(pos2)

	if stats1.ObjectCount != stats2.ObjectCount {
		t.Errorf("root object count mismatch: %d vs %d", stats1.ObjectCount, stats2.ObjectCount)
	}

	if stats1.TotalBytes != stats2.TotalBytes {
		t.Errorf("root total bytes mismatch: %d vs %d", stats1.TotalBytes, stats2.TotalBytes)
	}
}

func TestGetGeneratorConfig_PresetValues(t *testing.T) {
	tests := []struct {
		preset       string
		wantFanout   int
		wantMaxDepth int
	}{
		{"small", 5, 3},
		{"medium", 10, 5},
		{"large", 20, 8},
	}

	for _, tt := range tests {
		t.Run(tt.preset, func(t *testing.T) {
			cfg := seeder.GetGeneratorConfig(tt.preset, 1000)

			if cfg.PrefixFanout != tt.wantFanout {
				t.Errorf("fanout: want %d, got %d", tt.wantFanout, cfg.PrefixFanout)
			}

			if cfg.MaxDepth != tt.wantMaxDepth {
				t.Errorf("maxDepth: want %d, got %d", tt.wantMaxDepth, cfg.MaxDepth)
			}
		})
	}

	// Verify realistic uses S3RealisticConfig values
	realistic := seeder.GetGeneratorConfig("realistic", 1000)
	expected := benchutil.S3RealisticConfig(1000)

	if realistic.PrefixFanout != expected.PrefixFanout {
		t.Errorf("realistic fanout: want %d, got %d", expected.PrefixFanout, realistic.PrefixFanout)
	}

	if realistic.MaxDepth != expected.MaxDepth {
		t.Errorf("realistic maxDepth: want %d, got %d", expected.MaxDepth, realistic.MaxDepth)
	}
}

func TestRun_RejectsZeroAndNegativeCount(t *testing.T) {
	for _, count := range []int{0, -1} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			err := seeder.Run(seeder.Config{
				OutputDir: t.TempDir(),
				Count:     count,
				Objects:   10,
				Preset:    "small",
				Logger:    zerolog.Nop(),
			})
			if err == nil {
				t.Fatalf("expected error for count=%d, got nil", count)
			}
		})
	}
}

func TestRun_RejectsZeroAndNegativeObjects(t *testing.T) {
	for _, objects := range []int{0, -1} {
		t.Run(fmt.Sprintf("objects=%d", objects), func(t *testing.T) {
			err := seeder.Run(seeder.Config{
				OutputDir: t.TempDir(),
				Count:     1,
				Objects:   objects,
				Preset:    "small",
				Logger:    zerolog.Nop(),
			})
			if err == nil {
				t.Fatalf("expected error for objects=%d, got nil", objects)
			}
		})
	}
}

func TestRun_DistinctInventoryHashAcrossAllPairs(t *testing.T) {
	// Original test only checked adjacent pairs; this catches any
	// coincidental periodicity in the seed math by comparing every pair.
	tmpDir := t.TempDir()
	if err := seeder.Run(seeder.Config{
		OutputDir: tmpDir,
		Count:     4,
		Objects:   200,
		Preset:    "small",
		Seed:      0,
		Logger:    zerolog.Nop(),
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	hashes := make([]string, 4)
	for i := range hashes {
		id := fmt.Sprintf("inv-%03d", i+1)
		data, err := os.ReadFile(filepath.Join(tmpDir, id, "prefix_blob.bin"))
		if err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		h := sha256.Sum256(data)
		hashes[i] = hex.EncodeToString(h[:])
	}
	for i := range hashes {
		for j := i + 1; j < len(hashes); j++ {
			if hashes[i] == hashes[j] {
				t.Errorf("inv-%03d and inv-%03d collide", i+1, j+1)
			}
		}
	}
}
