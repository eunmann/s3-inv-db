package format

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

// The MPHF files are a pure function of the keys: a change to how the
// build computes them must not change one byte. These digests were taken
// from the build before its passes were fused and its rank, bucketing
// and slot layout reworked; a test failing here means the change altered
// the on-disk format, which takes a Version bump.

// goldenKeyHash gives the prefixes at the listed indices one shared key
// hash per group, so the build has fallback entries; every other prefix
// keeps its real hash.
func goldenKeyHash(prefixes []string, groups ...[]int) func(string) uint64 {
	shared := make(map[string]uint64)
	for g, group := range groups {
		for _, i := range group {
			shared[prefixes[i]] = 0x9e3779b97f4a7c15 * uint64(g+1)
		}
	}

	return func(s string) uint64 {
		if h, ok := shared[s]; ok {
			return h
		}

		return hashString(s)
	}
}

// fileDigest is the hex SHA-256 of the file at path.
func fileDigest(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:])
}

func TestMPHFFilesMatchGolden(t *testing.T) {
	cases := []struct {
		n             int
		groups        [][]int
		mph, mphSlots string
	}{
		// One-byte positions, one bucket.
		{
			n: 200, groups: [][]int{{3, 77, 150}},
			mph:      "17a93e43897991e2813de944e1fd41e1ab2baceafd177b7b5685fe0db6e7cdc7",
			mphSlots: "fa3419687ada50ecc474f11c9dfdda76037b61b58cf59f308cd5713d3349d7a4",
		},
		// Two-byte positions.
		{
			n: 5000, groups: [][]int{{0, 4999}, {10, 20, 30, 40}},
			mph:      "5a4a1fda6f6f26150a6dee6eb77be489892435ce0d24478c110275d20df99895",
			mphSlots: "0a0cf94b594e4d803d2162d4411c471366f109c37ce30c71cf490630f149e8df",
		},
		// Several slot buckets, level keys in scratch files, progress
		// blocks spanning chunks.
		{
			n: 400_000, groups: [][]int{{7, 50_007, 100_007, 399_999}, {11, 99_999}},
			mph:      "2d64879626a8600900c418d42c90726f42f236b8b526358847c81667116a4d52",
			mphSlots: "64f31a9d94dee7f6937dd291cf7fc3f0be50d6bbf2289f51daf8dcfaa2dd30ae",
		},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("n=%d", tc.n), func(t *testing.T) {
			prefixes := make([]string, tc.n)
			for i := range prefixes {
				prefixes[i] = prefixFromInt(i + 1)
			}
			keyHash := goldenKeyHash(prefixes, tc.groups...)
			dir := t.TempDir()
			m := buildWithKeyHash(t, dir, prefixes, keyHash)
			if len(m.mph.fallback) == 0 {
				t.Fatal("no fallback entries: the case must cover the fallback table")
			}
			if got := fileDigest(t, filepath.Join(dir, MPHFile)); got != tc.mph {
				t.Errorf("%s digest = %s, want %s", MPHFile, got, tc.mph)
			}
			if got := fileDigest(t, filepath.Join(dir, MPHFSlotsFile)); got != tc.mphSlots {
				t.Errorf("%s digest = %s, want %s", MPHFSlotsFile, got, tc.mphSlots)
			}
			assertLookups(t, m, keyHash, prefixes)
		})
	}
}

// TestBBHashFileMatchesGolden covers a table deep enough that its later
// levels run on every worker, with keys equal on every level.
func TestBBHashFileMatchesGolden(t *testing.T) {
	keys := randomKeys(2_000_000, 42)
	keys = append(keys, keys[5], keys[5], keys[1_000_000])
	fps := make([]uint64, len(keys))
	for i, k := range keys {
		fps[i] = k ^ uint64(i)
	}
	h, err := newBBHash(keys, func(leftovers []uint64) ([]bbFallback, error) {
		return fallbackEntries(leftovers, keys, fps)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(h.fallback) != 5 {
		t.Fatalf("%d fallback entries, want the 5 keys that share a hash", len(h.fallback))
	}
	const want = "9ba0f4545d9ae8ed1c41244c4df80bec860069d36a4125313e617104d54ae21d"
	if sum := sha256.Sum256(encodeBBHash(t, h)); hex.EncodeToString(sum[:]) != want {
		t.Errorf("%s digest = %x, want %s", MPHFile, sum, want)
	}
}

// TestMPHFRandomLookups looks up every member of a build over random
// prefixes, in random order, and random non-members.
func TestMPHFRandomLookups(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	seen := make(map[string]bool)
	var prefixes []string
	for len(prefixes) < 100_000 {
		p := fmt.Sprintf("%x/%x/", r.Uint32N(1<<20), r.Uint64())
		if !seen[p] {
			seen[p] = true
			prefixes = append(prefixes, p)
		}
	}
	m := buildWithKeyHash(t, t.TempDir(), prefixes, hashString)
	for _, i := range r.Perm(len(prefixes)) {
		if pos, ok := m.Lookup(prefixes[i]); !ok || pos != uint64(i) {
			t.Fatalf("Lookup(%q) = %d, %t; want %d", prefixes[i], pos, ok, i)
		}
	}
	for range 100_000 {
		p := fmt.Sprintf("%x/", r.Uint64())
		if pos, ok := m.Lookup(p); ok {
			t.Fatalf("Lookup(%q) = %d for a non-member", p, pos)
		}
	}
}
