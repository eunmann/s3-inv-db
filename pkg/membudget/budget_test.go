package membudget_test

import (
	"errors"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/membudget"
)

func TestParseHumanSize(t *testing.T) {
	tests := []struct {
		input   string
		want    uint64
		wantErr error
	}{
		{"1024", 1024, nil},
		{"100B", 100, nil},
		{"1KB", 1000, nil},
		{"1KiB", 1024, nil},
		{"1K", 1024, nil},
		{"1MB", 1000000, nil},
		{"1MiB", 1024 * 1024, nil},
		{"1M", 1024 * 1024, nil},
		{"1GB", 1000000000, nil},
		{"1GiB", 1024 * 1024 * 1024, nil},
		{"4GiB", 4 * 1024 * 1024 * 1024, nil},
		{"0.5GiB", 512 * 1024 * 1024, nil},
		{"", 0, membudget.ErrEmptySize},
		{"XYZ", 0, membudget.ErrInvalidSizeNumber},
		{"100XB", 0, membudget.ErrUnknownSizeSuffix},
	}

	for _, tt := range tests {
		got, err := membudget.ParseHumanSize(tt.input)
		if tt.wantErr != nil {
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("ParseHumanSize(%q) err = %v, want %v", tt.input, err, tt.wantErr)
			}

			continue
		}
		if err != nil {
			t.Errorf("ParseHumanSize(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ParseHumanSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
