package main

import (
	"errors"
	"flag"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/rs/zerolog"
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

// TestResolveRuntimeOptions_LevelCache: --level-cache wins over the
// file's level_cache, which wins over the default; "0" turns it off.
func TestResolveRuntimeOptions_LevelCache(t *testing.T) {
	str := func(s string) *string { return &s }
	cases := []struct {
		name string
		args []string
		file *appconfig.Config
		want int64
	}{
		{"default", nil, nil, handlers.DefaultLevelCacheBytes},
		{"file without key", nil, &appconfig.Config{}, handlers.DefaultLevelCacheBytes},
		{"file sets size", nil, &appconfig.Config{LevelCache: str("1GB")}, 1_000_000_000},
		{"file turns off", nil, &appconfig.Config{LevelCache: str("0")}, 0},
		{"flag sets size", []string{"--level-cache=32MiB"}, nil, 32 << 20},
		{"flag over file", []string{"--level-cache=32MiB"}, &appconfig.Config{LevelCache: str("0")}, 32 << 20},
		{"flag turns off over file", []string{"--level-cache=0"}, &appconfig.Config{LevelCache: str("1GB")}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			f := defineFlags(fs)
			if err := fs.Parse(c.args); err != nil {
				t.Fatalf("parse: %v", err)
			}
			explicit := map[string]bool{}
			fs.Visit(func(fl *flag.Flag) { explicit[fl.Name] = true })
			opts, err := resolveRuntimeOptions(f, c.file, explicit, zerolog.Nop())
			if err != nil {
				t.Fatalf("resolveRuntimeOptions: %v", err)
			}
			if opts.LevelCacheBytes != c.want {
				t.Errorf("LevelCacheBytes = %d, want %d", opts.LevelCacheBytes, c.want)
			}
		})
	}
}

func TestResolveRuntimeOptions_LevelCacheRejectsBadSize(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := defineFlags(fs)
	bad := "lots"
	_, err := resolveRuntimeOptions(f, &appconfig.Config{LevelCache: &bad}, map[string]bool{}, zerolog.Nop())
	if err == nil {
		t.Fatal("resolveRuntimeOptions accepted level_cache \"lots\"")
	}
}
