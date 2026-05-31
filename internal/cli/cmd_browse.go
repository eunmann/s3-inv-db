package cli

import (
	"flag"
	"fmt"
	"os"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

type browseChild struct {
	Prefix      string `json:"prefix"`
	ObjectCount uint64 `json:"object_count"`
	TotalBytes  uint64 `json:"total_bytes"`
}

type browseOutput struct {
	Parent   string        `json:"parent"`
	Depth    int           `json:"relative_depth"`
	Children []browseChild `json:"children"`
}

func runBrowse(args []string) error {
	fs := flag.NewFlagSet("browse", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to JSON config file")
	indexDir := fs.String("index", "", "index directory to query")
	parent := fs.String("prefix", "", "prefix to browse (empty = root)")
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

	positions, err := idx.DescendantsAtDepth(parentPos, *depth)
	if err != nil {
		return fmt.Errorf("descendants: %w", err)
	}

	children := make([]browseChild, 0, len(positions))
	for _, pos := range positions {
		name, err := idx.PrefixString(pos)
		if err != nil {
			return fmt.Errorf("prefix string: %w", err)
		}
		stats := idx.Stats(pos)
		children = append(children, browseChild{
			Prefix:      name,
			ObjectCount: stats.ObjectCount,
			TotalBytes:  stats.TotalBytes,
		})
	}

	resp := browseOutput{Parent: *parent, Depth: *depth, Children: children}

	if out == OutputJSON {
		return writeJSON(os.Stdout, resp)
	}

	fmt.Fprintf(os.Stdout, "Children of %q at depth %d (%d):\n", *parent, *depth, len(children))
	for _, c := range children {
		fmt.Fprintf(os.Stdout, "  %s — %d objects, %d bytes\n", c.Prefix, c.ObjectCount, c.TotalBytes)
	}

	return nil
}
