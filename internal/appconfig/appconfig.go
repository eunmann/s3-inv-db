// Package appconfig loads the optional JSON config file consumed by
// the server and CLI binaries. Each field is a pointer so callers can
// distinguish "not set" from "set to zero".
package appconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// ErrInventoryKeysRequired is returned by validate when an inventory
// entry has a blank source or name.
var ErrInventoryKeysRequired = errors.New("inventories[]: source and name are required")

// Config mirrors the JSON file shape. Pointer fields are nil when the
// key was absent from the file.
type Config struct {
	Addr       *string `json:"addr,omitempty"`
	Verbose    *bool   `json:"verbose,omitempty"`
	PrettyLogs *bool   `json:"pretty_logs,omitempty"`
	PriceTable *string `json:"price_table,omitempty"`

	S3Source *string `json:"s3_source,omitempty"`
	CacheDir *string `json:"cache_dir,omitempty"`

	AutoLoad                 *bool   `json:"auto_load,omitempty"`
	PollInterval             *string `json:"auto_load_poll_interval,omitempty"` // Go duration string
	MaxIndexDisk             *string `json:"max_index_disk,omitempty"`          // size string (e.g. "200GB")
	MaxConcurrentJobs        *int    `json:"max_concurrent_jobs,omitempty"`
	AutoLoadRetentionDefault *uint32 `json:"auto_load_retention_default,omitempty"`
	DiscoveryRefreshInterval *string `json:"discovery_refresh_interval,omitempty"` // Go duration string

	BuildEventLog *string `json:"build_event_log,omitempty"`
	QueryBatchMax *int    `json:"query_batch_max,omitempty"`

	Inventories []InventoryEntry `json:"inventories,omitempty"`
}

// InventoryEntry declares per-configuration auto-load settings.
type InventoryEntry struct {
	Source         string `json:"source"`
	Name           string `json:"name"`
	AutoLoad       bool   `json:"auto_load"`
	RetentionCount uint32 `json:"retention_count,omitempty"`
}

// Load returns an empty Config (not nil) when path is empty.
func Load(path string) (*Config, error) {
	if path == "" {
		return &Config{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	for i := range c.Inventories {
		e := &c.Inventories[i]
		if e.Source == "" || e.Name == "" {
			return nil, fmt.Errorf("validate config %s: %w", path, ErrInventoryKeysRequired)
		}
	}

	return &c, nil
}

// PickFile collapses the "flag > file > default" precedence: returns
// flagVal when the flag was set explicitly, otherwise *get(cfg) when
// cfg is non-nil and get returns non-nil, otherwise flagVal (which is
// the flag's default).
//
//nolint:ireturn // T is the caller's concrete type, not an interface to satisfy
func PickFile[T any](flagVal T, explicit bool, cfg *Config, get func(*Config) *T) T {
	if explicit {
		return flagVal
	}
	if cfg != nil {
		if p := get(cfg); p != nil {
			return *p
		}
	}

	return flagVal
}
