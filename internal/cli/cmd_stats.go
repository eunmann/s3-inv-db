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
	Index           string          `json:"index"`
	ManifestVersion int             `json:"manifest_version"`
	NodeCount       uint64          `json:"node_count"`
	MaxDepth        uint32          `json:"max_depth"`
	HasTierData     bool            `json:"has_tier_data"`
	TotalBytes      uint64          `json:"total_bytes_on_disk"`
	Files           []indexFileInfo `json:"files"`
	CreatedAt       string          `json:"created_at,omitempty"`
}

func runStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	indexDir := fs.String("index", "", "index directory to inspect")
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
	if idx, err := indexread.Open(*indexDir); err == nil {
		hasTier = idx.HasTierData()
		_ = idx.Close()
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

	return nil
}
