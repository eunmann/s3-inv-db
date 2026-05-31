package cli

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

type compareRow struct {
	Prefix     string `json:"prefix"`
	FromCount  uint64 `json:"from_count"`
	FromBytes  uint64 `json:"from_bytes"`
	ToCount    uint64 `json:"to_count"`
	ToBytes    uint64 `json:"to_bytes"`
	DeltaCount int64  `json:"delta_count"`
	DeltaBytes int64  `json:"delta_bytes"`
	Status     string `json:"status"`
}

type compareOutput struct {
	From   string       `json:"from"`
	To     string       `json:"to"`
	Prefix string       `json:"prefix"`
	Depth  int          `json:"relative_depth"`
	Rows   []compareRow `json:"rows"`
}

func runCompare(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to JSON config file")
	fromIdx := fs.String("from", "", "baseline index directory")
	toIdx := fs.String("to", "", "comparison index directory")
	prefix := fs.String("prefix", "", "prefix at which to compare (empty = root)")
	depth := fs.Int("depth", 1, "relative depth below prefix")
	logFlags := addLoggingFlags(fs)
	outputFlag := addOutputFlag(fs)

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	out, err := parseOutputFormat(*outputFlag)
	if err != nil {
		return err
	}

	if *fromIdx == "" {
		return ErrFromIndexRequired
	}
	if *toIdx == "" {
		return ErrToIndexRequired
	}

	fileCfg, err := appconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	initLogging(logFlags, fs, fileCfg)

	from, err := indexread.Open(*fromIdx)
	if err != nil {
		return fmt.Errorf("open from index: %w", err)
	}
	defer from.Close()

	to, err := indexread.Open(*toIdx)
	if err != nil {
		return fmt.Errorf("open to index: %w", err)
	}
	defer to.Close()

	fromChildren, err := childrenStats(from, *prefix, *depth)
	if err != nil {
		return fmt.Errorf("from children: %w", err)
	}
	toChildren, err := childrenStats(to, *prefix, *depth)
	if err != nil {
		return fmt.Errorf("to children: %w", err)
	}

	rows := diffChildren(fromChildren, toChildren)

	resp := compareOutput{From: *fromIdx, To: *toIdx, Prefix: *prefix, Depth: *depth, Rows: rows}

	if out == OutputJSON {
		return writeJSON(os.Stdout, resp)
	}

	fmt.Fprintf(os.Stdout, "Compare %s vs %s @ %q depth %d (%d rows):\n", *fromIdx, *toIdx, *prefix, *depth, len(rows))
	for _, r := range rows {
		fmt.Fprintf(os.Stdout, "  [%s] %s — count %d→%d (%+d), bytes %d→%d (%+d)\n",
			r.Status, r.Prefix, r.FromCount, r.ToCount, r.DeltaCount, r.FromBytes, r.ToBytes, r.DeltaBytes)
	}

	return nil
}

func childrenStats(idx *indexread.Index, prefix string, depth int) (map[string]indexread.Stats, error) {
	pos, ok := idx.Lookup(prefix)
	if !ok {
		return map[string]indexread.Stats{}, nil
	}
	positions, err := idx.DescendantsAtDepth(pos, depth)
	if err != nil {
		return nil, fmt.Errorf("descendants: %w", err)
	}
	out := make(map[string]indexread.Stats, len(positions))
	for _, p := range positions {
		name, err := idx.PrefixString(p)
		if err != nil {
			return nil, fmt.Errorf("prefix string: %w", err)
		}
		out[name] = idx.Stats(p)
	}

	return out, nil
}

func diffChildren(from, to map[string]indexread.Stats) []compareRow {
	names := make(map[string]struct{}, len(from)+len(to))
	for k := range from {
		names[k] = struct{}{}
	}
	for k := range to {
		names[k] = struct{}{}
	}
	keys := make([]string, 0, len(names))
	for k := range names {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rows := make([]compareRow, 0, len(keys))
	for _, k := range keys {
		f, fok := from[k]
		t, tok := to[k]
		status := "changed"
		switch {
		case !fok:
			status = "added"
		case !tok:
			status = "removed"
		case f == t:
			status = "unchanged"
		}
		rows = append(rows, compareRow{
			Prefix:     k,
			FromCount:  f.ObjectCount,
			FromBytes:  f.TotalBytes,
			ToCount:    t.ObjectCount,
			ToBytes:    t.TotalBytes,
			DeltaCount: safeDelta(t.ObjectCount, f.ObjectCount),
			DeltaBytes: safeDelta(t.TotalBytes, f.TotalBytes),
			Status:     status,
		})
	}

	return rows
}

// safeDelta returns to - from as int64, clamping to int64 bounds to avoid
// wrap-around on extreme magnitudes (uint64 inputs can exceed int64 range).
func safeDelta(toV, fromV uint64) int64 {
	if toV >= fromV {
		d := toV - fromV
		if d > math.MaxInt64 {
			return math.MaxInt64
		}

		return int64(d)
	}
	d := fromV - toV
	if d > math.MaxInt64 {
		return math.MinInt64
	}

	return -int64(d)
}
