// Package appconfig loads the optional JSON config file consumed by
// the server and CLI binaries. Each field is a pointer so callers can
// distinguish "not set" from "set to zero".
package appconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Config mirrors the JSON file shape. Pointer fields are nil when the
// key was absent from the file.
type Config struct {
	Addr       *string `json:"addr,omitempty"`
	Verbose    *bool   `json:"verbose,omitempty"`
	PrettyLogs *bool   `json:"pretty_logs,omitempty"`
	PriceTable *string `json:"price_table,omitempty"`

	S3Source   *string `json:"s3_source,omitempty"`
	CacheDir   *string `json:"cache_dir,omitempty"`
	ScratchDir *string `json:"scratch_dir,omitempty"`
	StateDB    *string `json:"state_db,omitempty"`

	AutoLoad                 *bool    `json:"auto_load,omitempty"`
	PollInterval             *string  `json:"auto_load_poll_interval,omitempty"` // Go duration string
	MaxIndexDisk             *string  `json:"max_index_disk,omitempty"`          // size string (e.g. "200GB")
	IndexHeadroom            *string  `json:"index_headroom,omitempty"`
	AutoLoadConcurrency      *int     `json:"max_auto_load_concurrency,omitempty"`
	AutoLoadRetentionDefault *uint32  `json:"auto_load_retention_default,omitempty"`
	IndexRatio               *float64 `json:"index_ratio,omitempty"`

	Inventories []InventoryEntry `json:"inventories,omitempty"`
}

// InventoryEntry declares per-configuration auto-load settings.
type InventoryEntry struct {
	Source         string `json:"source"`
	Name           string `json:"name"`
	AutoLoad       bool   `json:"auto_load"`
	RetentionCount uint32 `json:"retention_count,omitempty"`
}

// Load reads and parses the file at path. Returns nil if path is empty.
func Load(path string) (*Config, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	dec := json.NewDecoder(newReader(data))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("validate config %s: %w", path, err)
	}
	return &c, nil
}

func (c *Config) validate() error {
	if c == nil {
		return nil
	}
	for i := range c.Inventories {
		e := &c.Inventories[i]
		if e.Source == "" || e.Name == "" {
			return errors.New("inventories[]: source and name are required")
		}
	}
	return nil
}

// PickString returns flagVal if the flag was explicit, otherwise
// configVal (when non-nil), otherwise flagVal (which is already the
// env-or-default value).
func PickString(flagVal string, explicit bool, configVal *string) string {
	if explicit {
		return flagVal
	}
	if configVal != nil {
		return *configVal
	}
	return flagVal
}

func PickBool(flagVal, explicit bool, configVal *bool) bool {
	if explicit {
		return flagVal
	}
	if configVal != nil {
		return *configVal
	}
	return flagVal
}

func PickInt(flagVal int, explicit bool, configVal *int) int {
	if explicit {
		return flagVal
	}
	if configVal != nil {
		return *configVal
	}
	return flagVal
}

func PickUint32(flagVal uint32, explicit bool, configVal *uint32) uint32 {
	if explicit {
		return flagVal
	}
	if configVal != nil {
		return *configVal
	}
	return flagVal
}

func PickFloat64(flagVal float64, explicit bool, configVal *float64) float64 {
	if explicit {
		return flagVal
	}
	if configVal != nil {
		return *configVal
	}
	return flagVal
}
