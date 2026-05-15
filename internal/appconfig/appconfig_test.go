package appconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestLoad_EmptyPathReturnsNil(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\"): %v", err)
	}
	if cfg != nil {
		t.Errorf("expected nil config, got %+v", cfg)
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
		"max_auto_load_concurrency": 2,
		"auto_load_retention_default": 3,
		"index_ratio": 0.4,
		"inventories": [
			{"source": "bkt", "name": "daily", "auto_load": true, "retention_count": 5}
		]
	}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg == nil || cfg.Addr == nil || *cfg.Addr != ":9000" {
		t.Errorf("Addr not loaded: %+v", cfg.Addr)
	}
	if cfg.Verbose == nil || !*cfg.Verbose {
		t.Errorf("Verbose: %+v", cfg.Verbose)
	}
	if cfg.AutoLoadConcurrency == nil || *cfg.AutoLoadConcurrency != 2 {
		t.Errorf("AutoLoadConcurrency: %+v", cfg.AutoLoadConcurrency)
	}
	if len(cfg.Inventories) != 1 {
		t.Fatalf("Inventories len = %d", len(cfg.Inventories))
	}
	if cfg.Inventories[0].Source != "bkt" || cfg.Inventories[0].Name != "daily" {
		t.Errorf("Inventories[0]: %+v", cfg.Inventories[0])
	}
	if !cfg.Inventories[0].AutoLoad || cfg.Inventories[0].RetentionCount != 5 {
		t.Errorf("Inventories[0] flags: %+v", cfg.Inventories[0])
	}
}

func TestLoad_RejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, `{"addr": ":9000", "bogus": true}`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected unknown-field error")
	}
}

func TestLoad_RejectsBlankInventoryKeys(t *testing.T) {
	path := writeConfig(t, `{"inventories": [{"source": "", "name": "x"}]}`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "source and name") {
		t.Errorf("expected blank-key error, got %v", err)
	}
}

func TestPickString_PrecedenceOrder(t *testing.T) {
	cfgVal := "from-config"
	cases := []struct {
		name     string
		flagVal  string
		explicit bool
		cfg      *string
		want     string
	}{
		{"explicit flag wins", "from-flag", true, &cfgVal, "from-flag"},
		{"config beats env-default", "env-default", false, &cfgVal, "from-config"},
		{"env-default when no config", "env-default", false, nil, "env-default"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PickString(tc.flagVal, tc.explicit, tc.cfg)
			if got != tc.want {
				t.Errorf("PickString = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPickBool_PrecedenceOrder(t *testing.T) {
	tr := true
	if got := PickBool(false, true, &tr); got != false {
		t.Errorf("explicit false flag should override config true, got %v", got)
	}
	if got := PickBool(false, false, &tr); got != true {
		t.Errorf("non-explicit flag should pick up config true, got %v", got)
	}
	if got := PickBool(true, false, nil); got != true {
		t.Errorf("env-default true should win when no config, got %v", got)
	}
}
