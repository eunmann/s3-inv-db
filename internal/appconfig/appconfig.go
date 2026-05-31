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

// Load reads and parses the file at path. Returns an empty Config when
// path is empty so callers don't need a separate nil check before
// resolving precedence with CLI/env flags.
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

// Pick returns flagVal if the flag was explicit, otherwise configVal
// (when non-nil), otherwise flagVal (which is already the env-or-default
// value). Generic over every type a CLI flag carries in this repo.
//
//nolint:ireturn // T is the caller's concrete type, not an interface to satisfy
func Pick[T any](flagVal T, explicit bool, configVal *T) T {
	if explicit {
		return flagVal
	}
	if configVal != nil {
		return *configVal
	}

	return flagVal
}

// FromFile returns get(cfg) when cfg is non-nil, otherwise nil. Lets
// callers thread "config may be missing" without bespoke nil-guard
// helpers per field type.
func FromFile[T any](cfg *Config, get func(*Config) *T) *T {
	if cfg == nil {
		return nil
	}

	return get(cfg)
}
