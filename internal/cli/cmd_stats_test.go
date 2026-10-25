package cli_test

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/cli"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// captureStdout runs fn with os.Stdout redirected and returns what it
// wrote.
func captureStdout(t *testing.T, fn func() error) []byte {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	runErr := fn()
	os.Stdout = orig
	_ = w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if runErr != nil {
		t.Fatalf("run: %v\n%s", runErr, out)
	}

	return out
}

func TestStatsProfileJSON(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(2, 0)
	agg.AddObject("a/x.txt", 10, tiers.Standard)
	agg.AddObject("a/b/y.txt", 300, tiers.GlacierIR)
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	builder, err := extsort.NewIndexBuilder(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Finalize(); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() error {
		return cli.Run([]string{"stats", "--index", dir, "--profile", "--output", "json"})
	})
	var got struct {
		Profile *struct {
			Prefixes       uint64   `json:"prefixes"`
			TiersPerPrefix []uint64 `json:"tiers_per_prefix"`
		} `json:"profile"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %s: %v", out, err)
	}
	if got.Profile == nil || got.Profile.Prefixes != 3 {
		t.Fatalf("profile = %+v, want 3 prefixes", got.Profile)
	}
	// "" and "a/" hold two tiers, "a/b/" one.
	if got.Profile.TiersPerPrefix[1] != 1 || got.Profile.TiersPerPrefix[2] != 2 {
		t.Errorf("tiers_per_prefix = %v", got.Profile.TiersPerPrefix)
	}

	plain := captureStdout(t, func() error {
		return cli.Run([]string{"stats", "--index", dir})
	})
	if json.Valid(plain) {
		t.Error("text output parsed as JSON")
	}
}
