package format

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/eunmann/s3-inv-db/pkg/triebuild"
)

//nolint:gocyclo // Test covers multiple tier scenarios
func TestTierStatsWriteRead(t *testing.T) {
	dir := t.TempDir()

	// Create a mock trie result with tier data
	nodes := []triebuild.Node{
		{
			Prefix:      "",
			Pos:         0,
			ObjectCount: 3,
			TotalBytes:  600,
			TierBytes:   [tiers.NumTiers]uint64{tiers.Standard: 100, tiers.GlacierFR: 200, tiers.ITFrequent: 300},
			TierCounts:  [tiers.NumTiers]uint64{tiers.Standard: 1, tiers.GlacierFR: 1, tiers.ITFrequent: 1},
		},
		{
			Prefix:      "a/",
			Pos:         1,
			ObjectCount: 2,
			TotalBytes:  300,
			TierBytes:   [tiers.NumTiers]uint64{tiers.Standard: 100, tiers.GlacierFR: 200},
			TierCounts:  [tiers.NumTiers]uint64{tiers.Standard: 1, tiers.GlacierFR: 1},
		},
		{
			Prefix:      "b/",
			Pos:         2,
			ObjectCount: 1,
			TotalBytes:  300,
			TierBytes:   [tiers.NumTiers]uint64{tiers.ITFrequent: 300},
			TierCounts:  [tiers.NumTiers]uint64{tiers.ITFrequent: 1},
		},
	}

	result := &triebuild.Result{
		Nodes:        nodes,
		MaxDepth:     1,
		TrackTiers:   true,
		PresentTiers: []tiers.ID{tiers.Standard, tiers.GlacierFR, tiers.ITFrequent},
	}

	// Write tier stats
	writer, err := NewTierStatsWriter(dir)
	if err != nil {
		t.Fatalf("NewTierStatsWriter failed: %v", err)
	}

	if err := writer.Write(result); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify files exist
	tierDir := filepath.Join(dir, "tier_stats")
	expectedFiles := []string{
		"standard_bytes.u64",
		"standard_count.u64",
		"glacier_fr_bytes.u64",
		"glacier_fr_count.u64",
		"it_frequent_bytes.u64",
		"it_frequent_count.u64",
	}

	for _, f := range expectedFiles {
		path := filepath.Join(tierDir, f)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected file %s to exist: %v", f, err)
		}
	}

	// Verify tiers.json exists
	if _, err := os.Stat(filepath.Join(dir, "tiers.json")); err != nil {
		t.Errorf("tiers.json not created: %v", err)
	}

	// Read tier stats back
	reader, err := OpenTierStats(dir)
	if err != nil {
		t.Fatalf("OpenTierStats failed: %v", err)
	}
	defer reader.Close()

	if !reader.HasTierData() {
		t.Fatal("expected HasTierData to return true")
	}

	// Test GetBreakdown for root node
	breakdown := reader.GetBreakdown(0)
	if len(breakdown) != 3 {
		t.Errorf("expected 3 tiers in breakdown, got %d", len(breakdown))
	}

	// Verify values
	tierValues := make(map[string]TierBreakdown)
	for _, tb := range breakdown {
		tierValues[tb.TierName] = tb
	}

	if v, ok := tierValues["STANDARD"]; !ok || v.Bytes != 100 || v.ObjectCount != 1 {
		t.Errorf("STANDARD: got bytes=%d count=%d, want 100/1", v.Bytes, v.ObjectCount)
	}
	if v, ok := tierValues["GLACIER"]; !ok || v.Bytes != 200 || v.ObjectCount != 1 {
		t.Errorf("GLACIER: got bytes=%d count=%d, want 200/1", v.Bytes, v.ObjectCount)
	}
	if v, ok := tierValues["INTELLIGENT_TIERING_FREQUENT"]; !ok || v.Bytes != 300 || v.ObjectCount != 1 {
		t.Errorf("IT_FREQUENT: got bytes=%d count=%d, want 300/1", v.Bytes, v.ObjectCount)
	}

	// Test GetBreakdown for node 2 (only IT_FREQUENT)
	breakdown2 := reader.GetBreakdown(2)
	if len(breakdown2) != 1 {
		t.Errorf("expected 1 tier in breakdown for node 2, got %d", len(breakdown2))
	}
	if breakdown2[0].TierName != "INTELLIGENT_TIERING_FREQUENT" || breakdown2[0].Bytes != 300 {
		t.Errorf("node 2 breakdown incorrect: %+v", breakdown2[0])
	}
}

func TestTierStatsWriteRead_NoTierData(t *testing.T) {
	dir := t.TempDir()

	result := &triebuild.Result{
		Nodes:      []triebuild.Node{{Prefix: "", Pos: 0}},
		MaxDepth:   0,
		TrackTiers: false,
	}

	writer, err := NewTierStatsWriter(dir)
	if err != nil {
		t.Fatalf("NewTierStatsWriter failed: %v", err)
	}

	if err := writer.Write(result); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// tier_stats directory should not be created when no tier data
	tierDir := filepath.Join(dir, "tier_stats")
	if _, err := os.Stat(tierDir); !os.IsNotExist(err) {
		t.Error("tier_stats directory should not exist when TrackTiers is false")
	}

	// Read should return nil
	reader, err := OpenTierStats(dir)
	if err != nil {
		t.Fatalf("OpenTierStats failed: %v", err)
	}
	if reader != nil {
		t.Error("expected nil reader when no tier data")
		reader.Close()
	}
}

func TestTierStatsReader_MissingDir(t *testing.T) {
	dir := t.TempDir()

	reader, err := OpenTierStats(dir)
	if err != nil {
		t.Fatalf("OpenTierStats failed: %v", err)
	}
	if reader != nil {
		t.Error("expected nil reader when tiers.json doesn't exist")
	}
}

func TestGetBreakdownAll(t *testing.T) {
	dir := t.TempDir()

	nodes := []triebuild.Node{
		{
			Prefix:     "",
			Pos:        0,
			TierBytes:  [tiers.NumTiers]uint64{tiers.Standard: 100},
			TierCounts: [tiers.NumTiers]uint64{tiers.Standard: 1},
		},
	}

	result := &triebuild.Result{
		Nodes:        nodes,
		TrackTiers:   true,
		PresentTiers: []tiers.ID{tiers.Standard, tiers.GlacierFR},
	}

	writer, err := NewTierStatsWriter(dir)
	if err != nil {
		t.Fatalf("NewTierStatsWriter failed: %v", err)
	}
	if err := writer.Write(result); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	reader, err := OpenTierStats(dir)
	if err != nil {
		t.Fatalf("OpenTierStats failed: %v", err)
	}
	defer reader.Close()

	// GetBreakdown returns only tiers with data
	breakdown := reader.GetBreakdown(0)
	if len(breakdown) != 1 {
		t.Errorf("GetBreakdown: expected 1 tier, got %d", len(breakdown))
	}

	// GetBreakdownAll returns all present tiers including zeros
	breakdownAll := reader.GetBreakdownAll(0)
	if len(breakdownAll) != 2 {
		t.Errorf("GetBreakdownAll: expected 2 tiers, got %d", len(breakdownAll))
	}
}

func TestPresentTiers(t *testing.T) {
	dir := t.TempDir()

	result := &triebuild.Result{
		Nodes:        []triebuild.Node{{Prefix: "", Pos: 0}},
		TrackTiers:   true,
		PresentTiers: []tiers.ID{tiers.Standard, tiers.DeepArchive},
	}

	writer, err := NewTierStatsWriter(dir)
	if err != nil {
		t.Fatalf("NewTierStatsWriter failed: %v", err)
	}
	if err := writer.Write(result); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	reader, err := OpenTierStats(dir)
	if err != nil {
		t.Fatalf("OpenTierStats failed: %v", err)
	}
	defer reader.Close()

	present := reader.PresentTiers()
	if len(present) != 2 {
		t.Errorf("expected 2 present tiers, got %d", len(present))
	}
}
