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
