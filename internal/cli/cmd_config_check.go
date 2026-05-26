package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
)

// errConfigInvalid is returned when the config file fails to load or
// validate. Wrapped with %w + the underlying error string for context.
var errConfigInvalid = errors.New("config invalid")

type configCheckOutput struct {
	Path  string         `json:"path"`
	Valid bool           `json:"valid"`
	Error string         `json:"error,omitempty"`
	Set   map[string]any `json:"set,omitempty"`
}

func runConfigCheck(args []string) error {
	fs := flag.NewFlagSet("config-check", flag.ContinueOnError)
	configPath := fs.String("config", os.Getenv("S3INV_CONFIG"), "path to JSON config file")
	outputFlag := addOutputFlag(fs)

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	out, err := parseOutputFormat(*outputFlag)
	if err != nil {
		return err
	}

	result := configCheckOutput{Path: *configPath, Valid: false}
	cfg, loadErr := appconfig.Load(*configPath)
	if loadErr != nil {
		result.Error = loadErr.Error()
	} else {
		result.Valid = true
		result.Set = describeSetFields(cfg)
	}

	if out == OutputJSON {
		if err := writeJSON(os.Stdout, result); err != nil {
			return err
		}
	} else {
		printConfigCheckText(result)
	}

	if !result.Valid {
		return fmt.Errorf("%w: %s", errConfigInvalid, result.Error)
	}

	return nil
}

func describeSetFields(c *appconfig.Config) map[string]any {
	set := map[string]any{}
	addIfSet(set, "addr", c.Addr)
	addIfSet(set, "verbose", c.Verbose)
	addIfSet(set, "pretty_logs", c.PrettyLogs)
	addIfSet(set, "price_table", c.PriceTable)
	addIfSet(set, "s3_source", c.S3Source)
	addIfSet(set, "cache_dir", c.CacheDir)
	addIfSet(set, "state_db", c.StateDB)
	addIfSet(set, "auto_load", c.AutoLoad)
	addIfSet(set, "auto_load_poll_interval", c.PollInterval)
	addIfSet(set, "max_index_disk", c.MaxIndexDisk)
	addIfSet(set, "index_headroom", c.IndexHeadroom)
	addIfSet(set, "max_auto_load_concurrency", c.AutoLoadConcurrency)
	addIfSet(set, "auto_load_retention_default", c.AutoLoadRetentionDefault)
	addIfSet(set, "index_ratio", c.IndexRatio)
	addIfSet(set, "discovery_refresh_interval", c.DiscoveryRefreshInterval)
	addIfSet(set, "auto_load_dry_run", c.AutoLoadDryRun)
	addIfSet(set, "metrics_enabled", c.MetricsEnabled)
	addIfSet(set, "metrics_addr", c.MetricsAddr)
	addIfSet(set, "build_event_log", c.BuildEventLog)
	addIfSet(set, "query_batch_max", c.QueryBatchMax)
	if len(c.Inventories) > 0 {
		set["inventories"] = c.Inventories
	}

	return set
}

func addIfSet[T any](dst map[string]any, key string, val *T) {
	if val != nil {
		dst[key] = *val
	}
}

func printConfigCheckText(r configCheckOutput) {
	fmt.Fprintf(os.Stdout, "Config file: %s\n", r.Path)
	if !r.Valid {
		fmt.Fprintf(os.Stdout, "Status: INVALID\nError: %s\n", r.Error)

		return
	}
	fmt.Fprintln(os.Stdout, "Status: OK")
	if len(r.Set) == 0 {
		fmt.Fprintln(os.Stdout, "(no fields set)")

		return
	}
	fmt.Fprintln(os.Stdout, "Set fields:")
	for k, v := range r.Set {
		fmt.Fprintf(os.Stdout, "  %s = %v\n", k, v)
	}
}
