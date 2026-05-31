package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

type topEntry struct {
	Prefix      string `json:"prefix"`
	ObjectCount uint64 `json:"object_count"`
	TotalBytes  uint64 `json:"total_bytes"`
}

type topOutput struct {
	Parent  string     `json:"parent"`
	Depth   int        `json:"relative_depth"`
	By      string     `json:"by"`
	Results []topEntry `json:"results"`
}

const defaultTopCLILimit = 25

// ErrBadTopBy is returned when --by is neither "bytes" nor "count".
var ErrBadTopBy = errors.New("--by must be 'bytes' or 'count'")

func runTop(args []string) error {
	fs := flag.NewFlagSet("top", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to JSON config file")
	indexDir := fs.String("index", "", "index directory to query")
	parent := fs.String("parent", "", "parent prefix to rank descendants under (empty = root)")
	depth := fs.Int("depth", 1, "relative depth below parent")
	limit := fs.Int("limit", defaultTopCLILimit, "maximum results to return")
	by := fs.String("by", "bytes", "rank metric: bytes or count")
	minCount := fs.Uint64("min-count", 0, "filter: minimum object count")
	minBytes := fs.Uint64("min-bytes", 0, "filter: minimum total bytes")
	logFlags := addLoggingFlags(fs)
	outputFlag := addOutputFlag(fs)

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	out, err := parseOutputFormat(*outputFlag)
	if err != nil {
		return err
	}

	var metric indexread.TopMetric
	switch *by {
	case "bytes":
		metric = indexread.TopByBytesMetric
	case "count":
		metric = indexread.TopByCountMetric
	default:
		return fmt.Errorf("%w: %q", ErrBadTopBy, *by)
	}

	fileCfg, err := appconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	initLogging(logFlags, fs, fileCfg)

	if *indexDir == "" {
		return ErrIndexRequired
	}

	idx, err := indexread.Open(*indexDir)
	if err != nil {
		return fmt.Errorf("open index: %w", err)
	}
	defer idx.Close()

	parentPos, ok := idx.Lookup(*parent)
	if !ok {
		return fmt.Errorf("%w: %s", ErrPrefixNotFound, *parent)
	}

	results, err := idx.TopFiltered(parentPos, *depth, *limit, metric, indexread.Filter{MinCount: *minCount, MinBytes: *minBytes})
	if err != nil {
		return fmt.Errorf("top: %w", err)
	}

	entries := make([]topEntry, 0, len(results))
	for _, r := range results {
		name, err := idx.PrefixString(r.Pos)
		if err != nil {
			return fmt.Errorf("prefix string: %w", err)
		}
		entries = append(entries, topEntry{
			Prefix:      name,
			ObjectCount: r.Stats.ObjectCount,
			TotalBytes:  r.Stats.TotalBytes,
		})
	}

	resp := topOutput{Parent: *parent, Depth: *depth, By: *by, Results: entries}

	if out == OutputJSON {
		return writeJSON(os.Stdout, resp)
	}

	printTopText(resp)

	return nil
}

func printTopText(r topOutput) {
	fmt.Fprintf(os.Stdout, "Top %d under %q at depth %d by %s:\n", len(r.Results), r.Parent, r.Depth, r.By)
	for i, e := range r.Results {
		fmt.Fprintf(os.Stdout, "  %2d. %s — %d objects, %d bytes\n", i+1, e.Prefix, e.ObjectCount, e.TotalBytes)
	}
}
