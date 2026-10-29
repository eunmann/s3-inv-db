package extsort

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// The index files are a pure function of the rows added: a change to how
// the builder carries rows to its writers must not change one byte.
// These digests were taken at format version 15, before the level spool
// carried tier slots; a test failing here means the change altered the
// on-disk format, which takes a Version bump.

// goldenTierRows is a root and its tree of rows, in key order, each with
// data in each of the given tiers with probability p: rows with no tier,
// one or many, counts and bytes of every width, empty objects and,
// rarely, bytes without a count.
func goldenTierRows(ids []tiers.ID, p float64, seed uint64) []*PrefixRow {
	r := rand.New(rand.NewPCG(seed, uint64(len(ids))))
	fill := func(row *PrefixRow, p float64) *PrefixRow {
		for _, id := range ids {
			if r.Float64() >= p {
				continue
			}
			c, b := 1+r.Uint64N(1<<(1+r.UintN(30))), r.Uint64N(1<<(1+r.UintN(50)))
			switch r.IntN(20) {
			case 0:
				b = 0
			case 1:
				c = 0
				b++
			}
			row.TierCounts[id], row.TierBytes[id] = c, b
			row.Count += c
			row.TotalBytes += b
		}

		return row
	}
	rows := []*PrefixRow{fill(&PrefixRow{}, 1)}
	for i := range 50 {
		parent := fmt.Sprintf("d%03d/", i)
		rows = append(rows, fill(&PrefixRow{Prefix: parent, Depth: 1}, p))
		for j := range i * 37 % 98 {
			rows = append(rows, fill(&PrefixRow{Prefix: fmt.Sprintf("%sc%03d/", parent, j), Depth: 2}, p))
		}
	}

	return rows
}

// indexFiles lists every file under dir but the manifest (it holds the
// build time) with its SHA-256, in path order.
func indexFiles(t *testing.T, dir string) []string {
	t.Helper()
	root := os.DirFS(dir)
	var lines []string
	err := fs.WalkDir(root, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() == format.ManifestFile {
			return err
		}
		data, err := fs.ReadFile(root, path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		sum := sha256.Sum256(data)
		lines = append(lines, path+" "+hex.EncodeToString(sum[:]))

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(lines)

	return lines
}

func TestIndexFilesMatchGolden(t *testing.T) {
	three := []tiers.ID{tiers.GlacierIR, tiers.ExpressOneZone, tiers.Snow}
	cases := []struct {
		name string
		// declared is the present-tier set, every tier when nil.
		declared []tiers.ID
		ids      []tiers.ID
		p        float64
		// files are the tier files the layout the plan picks writes.
		files  []string
		digest string
	}{
		{
			name: "tiers=16/multi", ids: tiers.AllTierIDs(), p: 0.08,
			files:  []string{format.TierMultiFile, format.TierMultiMaskFile, format.TierMultiOffsetsFile, format.TierMultiRowsFile},
			digest: "0b04a35c8807fb3c9703d90c3408d58535c71a2d54d13763e373e60df27ebeb6",
		},
		{
			name: "tiers=16/dense", ids: tiers.AllTierIDs(), p: 0.98,
			files:  []string{format.TierDenseFile},
			digest: "e1f14da27925d70244b994337a75f19fe13d7ff1f74e7b5d2a35478f4d200b64",
		},
		{
			name: "tiers=3/multi", declared: three, ids: three, p: 0.15,
			files:  []string{format.TierMultiFile, format.TierMultiOffsetsFile, format.TierMultiRowsFile},
			digest: "ac9a6229f7bfabe4dfe514b5f0fffecef3b7ad75fbe005fba7561f37f13d75b8",
		},
		{
			name: "tiers=3/dense", declared: three, ids: three, p: 0.95,
			files:  []string{format.TierDenseFile},
			digest: "dce28bed73c9f9c6f500ea6da0084ddb507f51cc43d28cbaef475477c988286b",
		},
		{
			name: "tiers=1", declared: []tiers.ID{tiers.Outposts}, ids: []tiers.ID{tiers.Outposts}, p: 0.7,
			digest: "8b110ab9e8c5f7fb80f52ce6a6e363b826a9db4fdad63b62cce5ad68c948c3fc",
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "index")
			b, err := NewIndexBuilder(dir, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if tc.declared != nil {
				if err := b.SetPresentTiers(tc.declared); err != nil {
					t.Fatal(err)
				}
			}
			for _, row := range goldenTierRows(tc.ids, tc.p, uint64(i)) {
				if err := b.Add(row); err != nil {
					t.Fatalf("Add(%q): %v", row.Prefix, err)
				}
			}
			if err := b.Finalize(); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(filepath.Join(dir, format.TierStatsDir))
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			files := make([]string, 0, len(entries))
			for _, e := range entries {
				files = append(files, e.Name())
			}
			slices.Sort(tc.files)
			if !slices.Equal(files, tc.files) {
				t.Fatalf("tier files %q, want %q", files, tc.files)
			}
			lines := strings.Join(indexFiles(t, dir), "\n")
			sum := sha256.Sum256([]byte(lines))
			if got := hex.EncodeToString(sum[:]); got != tc.digest {
				t.Errorf("index digest %s, want %s; files:\n%s", got, tc.digest, lines)
			}
		})
	}
}
