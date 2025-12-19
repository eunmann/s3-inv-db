package cli

import (
	"strings"
	"testing"
)

func TestRunNoArgs(t *testing.T) {
	err := Run(nil)
	if err == nil {
		t.Fatal("expected error with no args")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("expected usage message, got: %v", err)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	err := Run([]string{"unknown"})
	if err == nil {
		t.Fatal("expected error with unknown command")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("expected 'unknown command' error, got: %v", err)
	}
}

func TestBuildMissingOut(t *testing.T) {
	err := Run([]string{"build", "--s3-manifest", "s3://bucket/manifest.json"})
	if err == nil {
		t.Fatal("expected error with missing --out")
	}
	if !strings.Contains(err.Error(), "--out") {
		t.Errorf("expected '--out' error, got: %v", err)
	}
}

func TestBuildMissingS3Manifest(t *testing.T) {
	err := Run([]string{"build", "--out", "/out"})
	if err == nil {
		t.Fatal("expected error with missing --s3-manifest")
	}
	if !strings.Contains(err.Error(), "--s3-manifest") {
		t.Errorf("expected '--s3-manifest' error, got: %v", err)
	}
}

func TestQueryMissingIndex(t *testing.T) {
	err := Run([]string{"query", "--prefix", "test/"})
	if err == nil {
		t.Fatal("expected error with missing --index")
	}
	if !strings.Contains(err.Error(), "--index") {
		t.Errorf("expected '--index' error, got: %v", err)
	}
}

func TestQueryMissingPrefix(t *testing.T) {
	err := Run([]string{"query", "--index", "/path/to/index"})
	if err == nil {
		t.Fatal("expected error with missing --prefix")
	}
	if !strings.Contains(err.Error(), "--prefix") {
		t.Errorf("expected '--prefix' error, got: %v", err)
	}
}
