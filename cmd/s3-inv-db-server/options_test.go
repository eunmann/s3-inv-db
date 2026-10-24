package main

import (
	"errors"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
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

func TestParseSize_BinaryVsDecimalSuffixDistinct(t *testing.T) {
	// "TiB" must resolve to the binary multiplier (2^40), not the
	// decimal one (10^12) — and "TB" must resolve to the decimal
	// multiplier, not binary. The lookup table is ordered IB-suffixes
	// before bare ones so a future bare "B" entry can't preempt them.
	binGot, err := parseSize("1TiB")
	if err != nil {
		t.Fatalf("parseSize(1TiB): %v", err)
	}
	if binGot != 1<<40 {
		t.Errorf("parseSize(1TiB) = %d, want %d (binary)", binGot, uint64(1)<<40)
	}
	decGot, err := parseSize("1TB")
	if err != nil {
		t.Fatalf("parseSize(1TB): %v", err)
	}
	if decGot != 1_000_000_000_000 {
		t.Errorf("parseSize(1TB) = %d, want 1e12 (decimal)", decGot)
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

func TestResolveTrustedProxies_Precedence(t *testing.T) {
	file := &appconfig.Config{TrustedProxies: []string{"10.0.0.0/8"}}
	cases := []struct {
		name     string
		flagVal  string
		explicit bool
		cfg      *appconfig.Config
		want     []string
	}{
		{"default_none", "", false, nil, nil},
		{"file_used_without_flag", "", false, file, []string{"10.0.0.0/8"}},
		{"explicit_flag_wins", "192.168.0.0/16, 172.16.0.1", true, file, []string{"192.168.0.0/16", "172.16.0.1/32"}},
		{"explicit_empty_flag_clears_file", "", true, file, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := resolveTrustedProxies(c.flagVal, c.explicit, c.cfg)
			if err != nil {
				t.Fatalf("resolveTrustedProxies: %v", err)
			}
			if len(got) != len(c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
			for i := range c.want {
				if got[i].String() != c.want[i] {
					t.Errorf("prefix %d = %s, want %s", i, got[i], c.want[i])
				}
			}
		})
	}
}
