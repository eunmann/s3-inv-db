// Package cli implements the command-line interface for s3-inv-db.
package cli

import (
	"errors"
	"flag"
	"fmt"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
)

// Sentinel errors keep `err113` happy and let callers test for the
// specific failure mode with errors.Is.
var (
	ErrUsage = errors.New("usage: s3-inv-db <command> [options]\n" +
		"commands: build, query, top, browse, compare, verify, stats, config-check")
	ErrUnknownCommand    = errors.New("unknown command")
	ErrOutRequired       = errors.New("--out is required")
	ErrManifestRequired  = errors.New("--s3-manifest is required")
	ErrIndexRequired     = errors.New("--index is required")
	ErrPrefixRequired    = errors.New("--prefix is required")
	ErrPrefixNotFound    = errors.New("prefix not found")
	ErrFromIndexRequired = errors.New("--from is required")
	ErrToIndexRequired   = errors.New("--to is required")
	ErrBadOutputFormat   = errors.New("invalid --output: must be 'text' or 'json'")
)

// Run executes the CLI with the given arguments.
func Run(args []string) error {
	if len(args) == 0 {
		return ErrUsage
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "build":
		return runBuild(cmdArgs)
	case "query":
		return runQuery(cmdArgs)
	case "top":
		return runTop(cmdArgs)
	case "browse":
		return runBrowse(cmdArgs)
	case "compare":
		return runCompare(cmdArgs)
	case "verify":
		return runVerify(cmdArgs)
	case "stats":
		return runStats(cmdArgs)
	case "config-check":
		return runConfigCheck(cmdArgs)
	default:
		return fmt.Errorf("%w: %s", ErrUnknownCommand, cmd)
	}
}

// OutputFormat selects between human-readable text and machine-readable JSON.
type OutputFormat string

const (
	OutputText OutputFormat = "text"
	OutputJSON OutputFormat = "json"
)

// addOutputFlag registers --output on the given FlagSet, defaulting to text.
func addOutputFlag(fs *flag.FlagSet) *string {
	return fs.String("output", string(OutputText), "output format: text or json")
}

// parseOutputFormat validates the --output value.
func parseOutputFormat(s string) (OutputFormat, error) {
	switch OutputFormat(s) {
	case OutputText:
		return OutputText, nil
	case OutputJSON:
		return OutputJSON, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrBadOutputFormat, s)
	}
}

func explicitFlags(fs *flag.FlagSet) map[string]bool {
	out := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { out[f.Name] = true })

	return out
}

func resolveBool(cfg *appconfig.Config, flagVal, explicit bool, get func(*appconfig.Config) *bool) bool {
	return appconfig.Pick(flagVal, explicit, appconfig.FromFile(cfg, get))
}

func resolveString(cfg *appconfig.Config, flagVal string, explicit bool, get func(*appconfig.Config) *string) string {
	return appconfig.Pick(flagVal, explicit, appconfig.FromFile(cfg, get))
}
