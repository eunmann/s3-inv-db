package sqliteagg

import (
	"os"
	"path/filepath"
	"testing"
)

/*
Test Organization:

Aggregator-specific tests (this file):
  - TestOpenClose: Database lifecycle (Open/Close)
  - TestExtractPrefixes: Unit test for prefix extraction
  - TestConfigValidate: Configuration validation

For end-to-end tests that write and read data, see e2e_correctness_test.go
which uses MemoryAggregator.
*/

func TestOpenClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := agg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify the database file was created
	if _, err := os.Stat(dbPath); err != nil {
		t.Errorf("database file not created: %v", err)
	}
}

func TestExtractPrefixes(t *testing.T) {
	tests := []struct {
		key      string
		expected []string
	}{
		{"file.txt", nil},
		{"a/file.txt", []string{"a/"}},
		{"a/b/file.txt", []string{"a/", "a/b/"}},
		{"a/b/c/file.txt", []string{"a/", "a/b/", "a/b/c/"}},
		{"a/b/c/", []string{"a/", "a/b/", "a/b/c/"}},
		{"", nil},
	}

	for _, tt := range tests {
		got := extractPrefixes(tt.key)
		if len(got) != len(tt.expected) {
			t.Errorf("extractPrefixes(%q) = %v, want %v", tt.key, got, tt.expected)
			continue
		}
		for i := range got {
			if got[i] != tt.expected[i] {
				t.Errorf("extractPrefixes(%q)[%d] = %q, want %q", tt.key, i, got[i], tt.expected[i])
			}
		}
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "valid default config",
			cfg:     DefaultConfig("/tmp/test.db"),
			wantErr: false,
		},
		{
			name:    "empty db path",
			cfg:     Config{},
			wantErr: true,
		},
		{
			name: "invalid synchronous",
			cfg: Config{
				DBPath:      "/tmp/test.db",
				Synchronous: "INVALID",
			},
			wantErr: true,
		},
		{
			name: "negative mmap size",
			cfg: Config{
				DBPath:   "/tmp/test.db",
				MmapSize: -1,
			},
			wantErr: true,
		},
		{
			name: "negative cache size",
			cfg: Config{
				DBPath:      "/tmp/test.db",
				CacheSizeKB: -1,
			},
			wantErr: true,
		},
		{
			name: "empty synchronous uses default",
			cfg: Config{
				DBPath:      "/tmp/test.db",
				Synchronous: "",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
