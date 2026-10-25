package cli

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

type indexFileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type statsOutput struct {
	Index           string             `json:"index"`
	ManifestVersion int                `json:"manifest_version"`
	NodeCount       uint64             `json:"node_count"`
	MaxDepth        uint32             `json:"max_depth"`
	HasTierData     bool               `json:"has_tier_data"`
	TotalBytes      uint64             `json:"total_bytes_on_disk"`
	Files           []indexFileInfo    `json:"files"`
	CreatedAt       string             `json:"created_at,omitempty"`
	Profile         *indexread.Profile `json:"profile,omitempty"`
}

func runStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	indexDir := fs.String("index", "", "index directory to inspect")
	profile := fs.Bool("profile", false, "scan every prefix and report the data's shape: tiers per prefix, value widths, tree shape, segments")
	outputFlag := addOutputFlag(fs)

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	out, err := parseOutputFormat(*outputFlag)
	if err != nil {
		return err
	}

	if *indexDir == "" {
		return ErrIndexRequired
	}

	manifest, err := format.ReadManifest(*indexDir)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	hasTier := false
	var prof *indexread.Profile
	idx, err := indexread.Open(*indexDir)
	switch {
	case err == nil:
		hasTier = idx.HasTierData()
		if *profile {
			prof = idx.Profile()
		}
		_ = idx.Close()
	case *profile:
		return fmt.Errorf("open index: %w", err)
	}

	files := make([]indexFileInfo, 0, len(manifest.Files))
	for name, info := range manifest.Files {
		files = append(files, indexFileInfo{Name: name, Size: info.Size})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Size > files[j].Size })

	result := statsOutput{
		Index:           *indexDir,
		ManifestVersion: manifest.Version,
		NodeCount:       manifest.NodeCount,
		MaxDepth:        manifest.MaxDepth,
		HasTierData:     hasTier,
		TotalBytes:      manifest.TotalBytes(),
		Files:           files,
		CreatedAt:       manifest.CreatedAt.Format("2006-01-02T15:04:05Z"),
		Profile:         prof,
	}

	if out == OutputJSON {
		return writeJSON(os.Stdout, result)
	}

	fmt.Fprintf(os.Stdout, "Index: %s\n", result.Index)
	fmt.Fprintf(os.Stdout, "Created: %s\n", result.CreatedAt)
	fmt.Fprintf(os.Stdout, "Manifest version: %d\n", result.ManifestVersion)
	fmt.Fprintf(os.Stdout, "Nodes: %d  MaxDepth: %d  TierData: %v\n", result.NodeCount, result.MaxDepth, result.HasTierData)
	fmt.Fprintf(os.Stdout, "Total on-disk bytes: %d\n", result.TotalBytes)
	fmt.Fprintln(os.Stdout, "Files (largest first):")
	for _, f := range result.Files {
		fmt.Fprintf(os.Stdout, "  %-40s  %d\n", f.Name, f.Size)
	}
	if prof != nil {
		printProfile(prof)
	}

	return nil
}

// Quantiles printProfile shows per value.
const (
	quantileMedian = 0.5
	quantileP99    = 0.99
	quantileP999   = 0.999
)

// printProfile writes the text form of an index profile.
func printProfile(p *indexread.Profile) {
	fmt.Fprintf(os.Stdout, "Profile: %d prefixes, %d leaves, %d segments (%d bytes)\n",
		p.Prefixes, p.Leaves, p.Segments, p.SegmentBytes)
	fmt.Fprintf(os.Stdout, "  prefixes per depth: %v\n", p.Depths)
	fmt.Fprintf(os.Stdout, "  prefixes by tiers held: %v (%d distinct tier sets)\n", p.TiersPerPrefix, p.TierSets)
	fmt.Fprintln(os.Stdout, "  bits per value (p50 / p99 / p99.9 / max):")
	names := make([]string, 0, len(p.Fields))
	for name := range p.Fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := p.Fields[name]
		fmt.Fprintf(os.Stdout, "    %-16s %2d / %2d / %2d / %2d  (%d values)\n", name,
			f.QuantileBits(quantileMedian), f.QuantileBits(quantileP99), f.QuantileBits(quantileP999), f.MaxBits(), f.Values)
	}
}
