package extsort

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// flatTierRows is a root and n depth-1 rows; row i holds tiersOf(i),
// one object and i bytes in each.
func flatTierRows(n int, tiersOf func(i int) []tiers.ID) []*PrefixRow {
	rows := make([]*PrefixRow, 1, 1+n)
	rows[0] = &PrefixRow{Prefix: ""}
	for i := range n {
		row := &PrefixRow{Prefix: fmt.Sprintf("p%06d/", i), Depth: 1}
		for _, id := range tiersOf(i) {
			row.TierCounts[id], row.TierBytes[id] = 1, uint64(i)
			row.Count++
			row.TotalBytes += uint64(i)
		}
		rows = append(rows, row)
		rows[0].Count += row.Count
		rows[0].TotalBytes += row.TotalBytes
		for id := range row.TierCounts {
			rows[0].TierCounts[id] += row.TierCounts[id]
			rows[0].TierBytes[id] += row.TierBytes[id]
		}
	}

	return rows
}

// deepTierRows is a random tree depth levels deep whose every row holds
// every tier of present.
func deepTierRows(depth int, present []tiers.ID) []*PrefixRow {
	prefixes := randomPrefixes(rand.New(rand.NewPCG(11, 7)), depth)
	rows := make([]*PrefixRow, 0, len(prefixes))
	for i, p := range prefixes {
		row := &PrefixRow{Prefix: p, Depth: uint16(strings.Count(p, "/"))}
		for _, id := range present {
			row.TierCounts[id], row.TierBytes[id] = uint64(1+i%3), uint64(i)
			row.Count += row.TierCounts[id]
			row.TotalBytes += row.TierBytes[id]
		}
		rows = append(rows, row)
	}

	return rows
}

// TestBuildManifestChecksumsTakenWhileWriting: across builds that
// write every optional file (core patches, dense and multi tier layouts,
// the multi layout's masks) and deep and wide trees, every file's
// manifest checksum was recorded by its writer, none was read back,
// and each matches the file read afresh.
func TestBuildManifestChecksumsTakenWhileWriting(t *testing.T) {
	three := []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}
	ten := make([]tiers.ID, 10)
	for i := range ten {
		ten[i] = tiers.ID(i)
	}
	cases := []struct {
		name    string
		present []tiers.ID
		rows    []*PrefixRow
	}{
		{"root only", []tiers.ID{tiers.Standard}, flatTierRows(0, nil)},
		{"patched core, wide", three, edgeTreeRows(3000, 900, 2000)},
		{"dense tiers, deep", three, deepTierRows(9, three)},
		{"multi tiers with masks, wide", ten, flatTierRows(5000, func(i int) []tiers.ID {
			if i%20 == 0 {
				return []tiers.ID{ten[i%10], ten[(i+3)%10], ten[(i+7)%10]}
			}

			return []tiers.ID{ten[i%10]}
		})},
	}
	seen := map[string]bool{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			b, err := NewIndexBuilder(dir, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := b.SetPresentTiers(c.present); err != nil {
				t.Fatal(err)
			}
			for _, row := range c.rows {
				if err := b.Add(row); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Finalize(); err != nil {
				t.Fatal(err)
			}

			reread, err := format.WriteManifest(dir, b.posCount, b.maxDepth, b.checksums, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(reread) > 0 {
				t.Errorf("files without a checksum from their writer: %v", reread)
			}
			m, err := format.ReadManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := format.VerifyManifest(dir, m); err != nil {
				t.Errorf("a recorded checksum differs from the file's: %v", err)
			}
			for name := range m.Files {
				seen[name] = true
			}
		})
	}

	tierFile := func(name string) string { return filepath.Join(format.TierStatsDir, name) }
	for _, name := range []string{
		format.DepthOffsetsFile, format.DepthBlocksFile, format.MPHFile, format.MPHFSlotsFile,
		tiers.ManifestFile, format.CoreStatsFile, format.PrefixDictBlobFile, format.PrefixDictOffsetsFile,
		format.PrefixTreeFile, format.CorePatchFile, format.CorePatchRowsFile,
		tierFile(format.TierDenseFile), tierFile(format.TierMultiFile), tierFile(format.TierMultiRowsFile),
		tierFile(format.TierMultiOffsetsFile), tierFile(format.TierMultiMaskFile),
	} {
		if !seen[name] {
			t.Errorf("no build wrote %s; files written: %v", name, slices.Sorted(maps.Keys(seen)))
		}
	}
}
