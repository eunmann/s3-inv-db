package main

import (
	"errors"
	"testing"
)

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"", 0},
		{"0", 0},
		{"42", 42},
		{"1024", 1024},
		{"1KB", 1000},
		{"1MB", 1_000_000},
		{"1GB", 1_000_000_000},
		{"1TB", 1_000_000_000_000},
		{"1KiB", 1024},
		{"1MiB", 1024 * 1024},
		{"1GiB", 1024 * 1024 * 1024},
		{"1TiB", 1024 * 1024 * 1024 * 1024},
		// Decimal multipliers and mixed-case suffixes.
		{"1.5GiB", uint64(1.5 * float64(1<<30))},
		{"2.5gb", 2_500_000_000},
		{"  16MiB  ", 16 * 1024 * 1024},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := parseSize(c.in)
			if err != nil {
				t.Fatalf("parseSize(%q): %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("parseSize(%q) = %d, want %d", c.in, got, c.want)
			}
		})
	}
}

func TestParseSize_LongestSuffixWins(t *testing.T) {
	// "TiB" must match before "B" — the loop is ordered longest-first.
	got, err := parseSize("1TiB")
	if err != nil {
		t.Fatalf("parseSize: %v", err)
	}
	if got != 1<<40 {
		t.Errorf("parseSize(1TiB) = %d, want %d", got, uint64(1)<<40)
	}
}

func TestParseSize_NoSuffixParsesRaw(t *testing.T) {
	got, err := parseSize("12345")
	if err != nil {
		t.Fatalf("parseSize: %v", err)
	}
	if got != 12345 {
		t.Errorf("parseSize(12345) = %d, want 12345", got)
	}
}

func TestParseSize_NegativeRejected(t *testing.T) {
	_, err := parseSize("-1MB")
	if err == nil {
		t.Fatal("parseSize accepted negative size")
	}
	if !errors.Is(err, errNegativeSize) {
		t.Errorf("err = %v, want errNegativeSize", err)
	}
}

func TestParseSize_MalformedRejected(t *testing.T) {
	_, err := parseSize("not-a-number")
	if err == nil {
		t.Fatal("parseSize accepted garbage")
	}
}
