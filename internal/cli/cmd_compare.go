package cli

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strings"

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

	fromLevel, err := levelAt(from, *prefix, *depth)
	if err != nil {
		return fmt.Errorf("from: %w", err)
	}
	toLevel, err := levelAt(to, *prefix, *depth)
	if err != nil {
		return fmt.Errorf("to: %w", err)
	}

	rows := diffLevels(fromLevel, toLevel)

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

// level is one index's prefixes at the compared depth, in index
// (preorder) order, which is byte order of the full prefix strings.
type level struct {
	idx   *indexread.Index
	pos   []uint64
	names []string
}

func levelAt(idx *indexread.Index, prefix string, depth int) (level, error) {
	l := level{idx: idx}
	pos, ok := idx.Lookup(prefix)
	if !ok {
		return l, nil
	}
	positions, err := idx.DescendantsAtDepth(pos, depth)
	if err != nil {
		return l, fmt.Errorf("descendants: %w", err)
	}
	l.pos = positions
	l.names = make([]string, len(positions))
	for i, p := range positions {
		if l.names[i], err = idx.PrefixString(p); err != nil {
			return l, fmt.Errorf("prefix string: %w", err)
		}
	}

	return l, nil
}

// diffLevels merges the two sorted levels. A prefix in both is changed
// when its objects, bytes or tier mix differ, as in the server's
// compare view.
func diffLevels(from, to level) []compareRow {
	rows := make([]compareRow, 0, max(len(from.pos), len(to.pos)))
	var tiersA, tiersB []indexread.TierBreakdown
	i, j := 0, 0
	for i < len(from.pos) || j < len(to.pos) {
		var c int
		switch {
		case i == len(from.pos):
			c = 1
		case j == len(to.pos):
			c = -1
		default:
			c = strings.Compare(from.names[i], to.names[j])
		}
		var f, t indexread.Stats
		row := compareRow{}
		switch {
		case c < 0:
			f = from.idx.Stats(from.pos[i])
			row.Prefix, row.Status = from.names[i], "removed"
			i++
		case c > 0:
			t = to.idx.Stats(to.pos[j])
			row.Prefix, row.Status = to.names[j], "added"
			j++
		default:
			f, t = from.idx.Stats(from.pos[i]), to.idx.Stats(to.pos[j])
			row.Prefix, row.Status = from.names[i], "unchanged"
			if f != t {
				row.Status = "changed"
			} else {
				tiersA = from.idx.AppendTierBreakdown(tiersA[:0], from.pos[i])
				tiersB = to.idx.AppendTierBreakdown(tiersB[:0], to.pos[j])
				if !indexread.TiersEqual(tiersA, tiersB) {
					row.Status = "changed"
				}
			}
			i++
			j++
		}
		row.FromCount, row.FromBytes = f.ObjectCount, f.TotalBytes
		row.ToCount, row.ToBytes = t.ObjectCount, t.TotalBytes
		row.DeltaCount = safeDelta(t.ObjectCount, f.ObjectCount)
		row.DeltaBytes = safeDelta(t.TotalBytes, f.TotalBytes)
		rows = append(rows, row)
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
