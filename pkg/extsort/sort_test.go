package extsort

import (
	"testing"
)

func TestConfigType(t *testing.T) {
	cfg := Config{
		MaxRecordsPerChunk: 1000,
		TmpDir:             "/tmp",
	}
	if cfg.MaxRecordsPerChunk != 1000 {
		t.Errorf("MaxRecordsPerChunk = %d, want 1000", cfg.MaxRecordsPerChunk)
	}
	if cfg.TmpDir != "/tmp" {
		t.Errorf("TmpDir = %s, want /tmp", cfg.TmpDir)
	}
}
