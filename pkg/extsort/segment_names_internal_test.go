package extsort

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// dirPrefixes is the root and every directory prefix of keys, in key
// order.
func dirPrefixes(keys []string) []string {
	set := map[string]bool{"": true}
	for _, k := range keys {
		for i := range len(k) {
			if k[i] == '/' {
				set[k[:i+1]] = true
			}
		}
	}

	return slices.Sorted(maps.Keys(set))
}

func generatedKeys(cfg benchutil.GeneratorConfig) []string {
	objects := benchutil.NewGenerator(cfg).Generate()
	keys := make([]string, len(objects))
	for i, o := range objects {
		keys[i] = o.Key
	}

	return keys
}

// adversarialKeys have names that repeat across depths and parents, are
// empty, long, differ only in a last byte or in length, or hold bytes a
// hash or comparison could trip on.
func adversarialKeys() []string {
	long := strings.Repeat("L", 1000)
	keys := make([]string, 0, 320)
	keys = append(keys,
		"a/a/a/a/a/a/a/a/f", "a/b/a/b/a/f", "a//b//f", "//f", "x///f",
		long+"/"+long+"/f", long+"x/"+long+"/f", "a/"+long+"/a/f",
		"日本/語/日本/f", "\x00/\x00\x00/f", "\xff/\xfe/\xff/f", "d/2024/f", "d/2024-/f", "d/2024./f",
	)
	for i := range 300 {
		keys = append(keys, "rep/"+strings.Repeat("n", i%7)+"/same/same/f")
	}

	return keys
}

// buildPrefixes builds an index of prefixes, whose interner remembers at
// most budget bytes (the builder's own budget when 0).
func buildPrefixes(t *testing.T, prefixes []string, budget int64) string {
	t.Helper()
	dir := t.TempDir()
	b, err := NewIndexBuilder(dir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if budget > 0 {
		if err := b.interner.Abort(); err != nil {
			t.Fatal(err)
		}
		if b.interner, err = format.NewPrefixSegmentInterner(dir, budget, b.checksums); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range prefixes {
		row := &PrefixRow{Prefix: p, Depth: uint16(strings.Count(p, "/")), Count: 1, TotalBytes: 1}
		row.TierCounts[tiers.Standard], row.TierBytes[tiers.Standard] = 1, 1
		if err := b.Add(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Finalize(); err != nil {
		t.Fatal(err)
	}

	return dir
}

// TestEveryPrefixNameRoundTrips: every prefix of realistic, deep,
// per-object-directory and adversarial trees reads back exactly, its
// listing name included, whether the interner holds every distinct
// segment or spills them many times over. Held, the dictionary is each
// distinct segment once; spilled, it may repeat some, never more than
// one entry per prefix.
func TestEveryPrefixNameRoundTrips(t *testing.T) {
	shapes := map[string][]string{
		"realistic":   generatedKeys(benchutil.S3RealisticConfig(20_000)),
		"deep":        generatedKeys(benchutil.S3DeepPyramidConfig(20_000)),
		"unique_dirs": generatedKeys(benchutil.S3UniqueDirsConfig(20_000)),
		"adversarial": adversarialKeys(),
	}
	for name, keys := range shapes {
		prefixes := dirPrefixes(keys)
		distinct := map[string]bool{}
		for _, p := range prefixes[1:] {
			distinct[lastSegment(p)] = true
		}
		for _, budget := range []int64{0, 16 << 10} {
			dir := buildPrefixes(t, prefixes, budget)
			levelOrder := slices.Clone(prefixes)
			slices.SortStableFunc(levelOrder, func(a, b string) int { return strings.Count(a, "/") - strings.Count(b, "/") })
			tree, err := format.OpenPrefixTreeReader(dir)
			if err != nil {
				t.Fatal(err)
			}
			for i, want := range levelOrder {
				got, err := tree.Prefix(uint64(i))
				if err != nil || got != want {
					t.Fatalf("%s, budget %d: Prefix(%d) = %q, %v; want %q", name, budget, i, got, err, want)
				}
				if i > 0 && tree.ChildSegment(uint64(i)) != lastSegment(want) {
					t.Fatalf("%s, budget %d: ChildSegment(%d) = %q, want %q", name, budget, i, tree.ChildSegment(uint64(i)), lastSegment(want))
				}
			}
			entries := tree.SegmentStats().Count
			if err := tree.Close(); err != nil {
				t.Fatal(err)
			}
			if budget == 0 && entries != uint64(len(distinct)) {
				t.Errorf("%s: %d dictionary entries, want the %d distinct segments", name, entries, len(distinct))
			}
			if entries < uint64(len(distinct)) || entries > uint64(len(prefixes)-1) {
				t.Errorf("%s, budget %d: %d dictionary entries for %d distinct segments and %d prefixes",
					name, budget, entries, len(distinct), len(prefixes)-1)
			}
		}
	}
}

func lastSegment(prefix string) string {
	return prefix[strings.LastIndexByte(prefix[:len(prefix)-1], '/')+1 : len(prefix)-1]
}

// TestInternerMemoryBudget: the interner takes its share of a memory
// limit, and its default without one.
func TestInternerMemoryBudget(t *testing.T) {
	for _, tc := range []struct{ limit, want int64 }{
		{0, defaultInternerBudget},
		{unsetMemoryLimit, defaultInternerBudget},
		{1 << 62, defaultInternerBudget},
		{4 << 30, 256 << 20},
		{512 << 20, 32 << 20},
	} {
		if got := internerMemoryBudget(tc.limit); got != tc.want {
			t.Errorf("internerMemoryBudget(%d) = %d, want %d", tc.limit, got, tc.want)
		}
	}
}
