package appconfig_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
)

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	return path
}

func TestLoad_EmptyPathReturnsEmptyConfig(t *testing.T) {
	cfg, err := appconfig.Load("")
	if err != nil {
		t.Fatalf("appconfig.Load(\"\"): %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil empty *Config, got nil")
	}
	if cfg.Addr != nil || cfg.Verbose != nil || cfg.PrettyLogs != nil ||
		cfg.PriceTable != nil || len(cfg.Inventories) != 0 {
		t.Errorf("expected zero-value Config, got %+v", cfg)
	}
}

func TestLoad_FullExample(t *testing.T) {
	path := writeConfig(t, `{
		"addr": ":9000",
		"verbose": true,
		"price_table": "/tmp/prices.json",
		"auto_load": true,
		"auto_load_poll_interval": "5m",
		"max_index_disk": "200GB",
		"max_concurrent_jobs": 2,
		"auto_load_retention_default": 3,
		"inventories": [
			{"source": "bkt", "name": "daily", "auto_load": true, "retention_count": 5}
		]
	}`)
	cfg, err := appconfig.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg == nil || cfg.Addr == nil || *cfg.Addr != ":9000" {
		t.Errorf("Addr not loaded: %+v", cfg.Addr)
	}
	if cfg.Verbose == nil || !*cfg.Verbose {
		t.Errorf("Verbose: %+v", cfg.Verbose)
	}
	if cfg.MaxConcurrentJobs == nil || *cfg.MaxConcurrentJobs != 2 {
		t.Errorf("MaxConcurrentJobs: %+v", cfg.MaxConcurrentJobs)
	}
	if len(cfg.Inventories) != 1 {
		t.Fatalf("Inventories len = %d", len(cfg.Inventories))
	}
	if cfg.Inventories[0].Source != "bkt" || cfg.Inventories[0].Name != "daily" {
		t.Errorf("Inventories[0]: %+v", cfg.Inventories[0])
	}
	if al := cfg.Inventories[0].AutoLoad; al == nil || !*al || cfg.Inventories[0].RetentionCount != 5 {
		t.Errorf("Inventories[0] flags: %+v", cfg.Inventories[0])
	}
}

// TestLoad_InventoryAutoLoadOmittedIsNil: an entry that leaves out
// auto_load leaves it to the UI; an explicit false is set.
func TestLoad_InventoryAutoLoadOmittedIsNil(t *testing.T) {
	path := writeConfig(t, `{"inventories": [{"source": "a", "name": "x", "retention_count": 3}, {"source": "b", "name": "y", "auto_load": false}]}`)
	cfg, err := appconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Inventories[0].AutoLoad != nil {
		t.Errorf("omitted auto_load = %v, want nil", *cfg.Inventories[0].AutoLoad)
	}
	if al := cfg.Inventories[1].AutoLoad; al == nil || *al {
		t.Errorf("explicit auto_load false = %v, want set to false", al)
	}
}

func TestLoad_RejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, `{"addr": ":9000", "bogus": true}`)
	if _, err := appconfig.Load(path); err == nil {
		t.Fatal("expected unknown-field error")
	}
}

func TestLoad_RejectsBlankInventoryKeys(t *testing.T) {
	path := writeConfig(t, `{"inventories": [{"source": "", "name": "x"}]}`)
	_, err := appconfig.Load(path)
	if err == nil || !strings.Contains(err.Error(), "source and name") {
		t.Errorf("expected blank-key error, got %v", err)
	}
}

func TestPickFile_String_PrecedenceOrder(t *testing.T) {
	addr := "from-config"
	cfgWithVal := &appconfig.Config{Addr: &addr}
	cases := []struct {
		name     string
		flagVal  string
		explicit bool
		cfg      *appconfig.Config
		want     string
	}{
		{"explicit flag wins", "from-flag", true, cfgWithVal, "from-flag"},
		{"config beats flag default", "default", false, cfgWithVal, "from-config"},
		{"flag default when no config", "default", false, nil, "default"},
		{"flag default when config nil-field", "default", false, &appconfig.Config{}, "default"},
	}
	get := func(c *appconfig.Config) *string { return c.Addr }
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := appconfig.PickFile(tc.flagVal, tc.explicit, tc.cfg, get)
			if got != tc.want {
				t.Errorf("PickFile = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPickFile_Bool_PrecedenceOrder(t *testing.T) {
	get := func(c *appconfig.Config) *bool { return c.Verbose }
	tr := true
	cfgTrue := &appconfig.Config{Verbose: &tr}
	if got := appconfig.PickFile(false, true, cfgTrue, get); got != false {
		t.Errorf("explicit false flag should override config true, got %v", got)
	}
	if got := appconfig.PickFile(false, false, cfgTrue, get); got != true {
		t.Errorf("non-explicit flag should pick up config true, got %v", got)
	}
	if got := appconfig.PickFile(true, false, nil, get); got != true {
		t.Errorf("flag default true should win when no config, got %v", got)
	}
}

func TestParsePrefixes_CIDRsAndBareIPs(t *testing.T) {
	got, err := appconfig.ParsePrefixes([]string{"10.0.0.0/8", " 192.168.1.7 ", "", "2001:db8::/32", "10.1.2.3/8"})
	if err != nil {
		t.Fatalf("ParsePrefixes: %v", err)
	}
	want := []string{"10.0.0.0/8", "192.168.1.7/32", "2001:db8::/32", "10.0.0.0/8"}
	if len(got) != len(want) {
		t.Fatalf("got %d prefixes %v, want %v", len(got), got, want)
	}
	for i := range want {
		if got[i].String() != want[i] {
			t.Errorf("prefix %d = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestParsePrefixes_RejectsGarbage(t *testing.T) {
	for _, bad := range []string{"proxy.internal", "10.0.0.0/33", "300.1.1.1"} {
		if _, err := appconfig.ParsePrefixes([]string{bad}); !errors.Is(err, appconfig.ErrInvalidPrefix) {
			t.Errorf("ParsePrefixes(%q) err = %v, want ErrInvalidPrefix", bad, err)
		}
	}
}

func TestLoad_RejectsInvalidTrustedProxies(t *testing.T) {
	path := writeConfig(t, `{"trusted_proxies": ["not-an-ip"]}`)
	if _, err := appconfig.Load(path); !errors.Is(err, appconfig.ErrInvalidPrefix) {
		t.Fatalf("Load err = %v, want ErrInvalidPrefix", err)
	}
}
