package cli

import (
	"os"
	"path/filepath"
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
	err := Run([]string{"build", "--tmp", "/tmp"})
	if err == nil {
		t.Fatal("expected error with missing --out")
	}
	if !strings.Contains(err.Error(), "--out") {
		t.Errorf("expected '--out' error, got: %v", err)
	}
}

func TestBuildMissingTmp(t *testing.T) {
	err := Run([]string{"build", "--out", "/out"})
	if err == nil {
		t.Fatal("expected error with missing --tmp")
	}
	if !strings.Contains(err.Error(), "--tmp") {
		t.Errorf("expected '--tmp' error, got: %v", err)
	}
}

func TestBuildMissingFiles(t *testing.T) {
	err := Run([]string{"build", "--out", "/out", "--tmp", "/tmp"})
	if err == nil {
		t.Fatal("expected error with no inventory files")
	}
	if !strings.Contains(err.Error(), "inventory file") {
		t.Errorf("expected 'inventory file' error, got: %v", err)
	}
}

func TestBuildSucceeds(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	// Create test inventory
	invPath := filepath.Join(tmpDir, "test.csv")
	csv := `Key,Size
a/file.txt,100
b/file.txt,200
`
	if err := os.WriteFile(invPath, []byte(csv), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	err := Run([]string{"build", "--out", outDir, "--tmp", sortDir, invPath})
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Check output directory exists
	if _, err := os.Stat(outDir); err != nil {
		t.Errorf("Output directory not created: %v", err)
	}

	// Check manifest exists
	manifestPath := filepath.Join(outDir, "manifest.json")
	if _, err := os.Stat(manifestPath); err != nil {
		t.Errorf("Manifest not created: %v", err)
	}
}
