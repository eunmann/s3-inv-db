package cli_test

import (
	"bytes"
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

func TestStatsProfile(t *testing.T) {
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
	if bytes.Contains(plain, []byte("Profile:")) {
		t.Errorf("text output without --profile has a profile:\n%s", plain)
	}

	text := captureStdout(t, func() error {
		return cli.Run([]string{"stats", "--index", dir, "--profile"})
	})
	// Object counts 2, 2 and 1 need 2, 2 and 1 bits; tier bytes 10, 300,
	// 10, 300 and 300 need 4, 9, 4, 9 and 9.
	for _, want := range []string{
		"Profile: 3 prefixes, 1 leaves,",
		"  prefixes per depth: [1 1 1]\n",
		"  prefixes by tiers held: [0 1 2 ",
		"  bits per value (p50 / p99 / p99.9 / max):\n",
		"    object_count      2 /  2 /  2 /  2  (3 values)\n",
		"    tier_bytes        9 /  9 /  9 /  9  (5 values)\n",
	} {
		if !bytes.Contains(text, []byte(want)) {
			t.Errorf("text profile lacks %q:\n%s", want, text)
		}
	}
}
