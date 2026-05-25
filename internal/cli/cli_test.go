package cli_test

import (
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/cli"
)

func TestRunNoArgs(t *testing.T) {
	err := cli.Run(nil)
	if err == nil {
		t.Fatal("expected error with no args")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("expected usage message, got: %v", err)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	err := cli.Run([]string{"unknown"})
	if err == nil {
		t.Fatal("expected error with unknown command")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("expected 'unknown command' error, got: %v", err)
	}
}

func TestBuildMissingOut(t *testing.T) {
	err := cli.Run([]string{"build", "--s3-manifest", "s3://bucket/manifest.json"})
	if err == nil {
		t.Fatal("expected error with missing --out")
	}
	if !strings.Contains(err.Error(), "--out") {
		t.Errorf("expected '--out' error, got: %v", err)
	}
}

func TestBuildMissingS3Manifest(t *testing.T) {
	err := cli.Run([]string{"build", "--out", "/out"})
	if err == nil {
		t.Fatal("expected error with missing --s3-manifest")
	}
	if !strings.Contains(err.Error(), "--s3-manifest") {
		t.Errorf("expected '--s3-manifest' error, got: %v", err)
	}
}

func TestQueryMissingIndex(t *testing.T) {
	err := cli.Run([]string{"query", "--prefix", "test/"})
	if err == nil {
		t.Fatal("expected error with missing --index")
	}
	if !strings.Contains(err.Error(), "--index") {
		t.Errorf("expected '--index' error, got: %v", err)
	}
}

func TestQueryMissingPrefix(t *testing.T) {
	err := cli.Run([]string{"query", "--index", "/path/to/index"})
	if err == nil {
		t.Fatal("expected error with missing --prefix")
	}
	if !strings.Contains(err.Error(), "--prefix") {
		t.Errorf("expected '--prefix' error, got: %v", err)
	}
}

func TestLoadPriceTable_DefaultsWhenEmpty(t *testing.T) {
	pt, err := cli.LoadPriceTable("")
	if err != nil {
		t.Fatalf("cli.LoadPriceTable(empty): %v", err)
	}
	if len(pt.PerGBMonth) == 0 {
		t.Error("default price table has no per-GB rates")
	}
}

func TestLoadPriceTable_MissingFile(t *testing.T) {
	_, err := cli.LoadPriceTable("/no/such/file.json")
	if err == nil {
		t.Fatal("loadPriceTable should error on missing file")
	}
	if !strings.Contains(err.Error(), "load price table") {
		t.Errorf("error should wrap with context, got: %v", err)
	}
}

// Missing-flag parity tests for the new subcommands. Each verifies the
// flag-required error path so a future refactor that drops a check
// breaks loudly.

func TestTopMissingIndex(t *testing.T) {
	err := cli.Run([]string{"top"})
	if err == nil || !strings.Contains(err.Error(), "--index") {
		t.Errorf("expected --index error, got: %v", err)
	}
}

func TestTopBadBy(t *testing.T) {
	err := cli.Run([]string{"top", "--index", "/i", "--by", "garbage"})
	if err == nil || !strings.Contains(err.Error(), "--by") {
		t.Errorf("expected --by error, got: %v", err)
	}
}

func TestBrowseMissingIndex(t *testing.T) {
	err := cli.Run([]string{"browse"})
	if err == nil || !strings.Contains(err.Error(), "--index") {
		t.Errorf("expected --index error, got: %v", err)
	}
}

func TestCompareMissingFrom(t *testing.T) {
	err := cli.Run([]string{"compare", "--to", "/b"})
	if err == nil || !strings.Contains(err.Error(), "--from") {
		t.Errorf("expected --from error, got: %v", err)
	}
}

func TestCompareMissingTo(t *testing.T) {
	err := cli.Run([]string{"compare", "--from", "/a"})
	if err == nil || !strings.Contains(err.Error(), "--to") {
		t.Errorf("expected --to error, got: %v", err)
	}
}

func TestVerifyMissingIndex(t *testing.T) {
	err := cli.Run([]string{"verify"})
	if err == nil || !strings.Contains(err.Error(), "--index") {
		t.Errorf("expected --index error, got: %v", err)
	}
}

func TestVerifyRejectsNegativeSample(t *testing.T) {
	err := cli.Run([]string{"verify", "--index", "/i", "--sample", "-1"})
	if err == nil || !strings.Contains(err.Error(), "--sample") {
		t.Errorf("expected --sample error, got: %v", err)
	}
}

func TestStatsMissingIndex(t *testing.T) {
	err := cli.Run([]string{"stats"})
	if err == nil || !strings.Contains(err.Error(), "--index") {
		t.Errorf("expected --index error, got: %v", err)
	}
}

func TestSubcommandBadOutputFormat(t *testing.T) {
	// One subcommand is enough; the validation is shared.
	err := cli.Run([]string{"verify", "--index", "/i", "--output", "yaml"})
	if err == nil || !strings.Contains(err.Error(), "--output") {
		t.Errorf("expected --output error, got: %v", err)
	}
}

func TestConfigCheckOnEmptyPath(t *testing.T) {
	// Empty --config should succeed: an empty Config is valid.
	if err := cli.Run([]string{"config-check"}); err != nil {
		t.Errorf("config-check on empty path should succeed, got: %v", err)
	}
}
