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
// from the partitioned build of format version 14; a test failing here
// means the change altered the on-disk format, which takes a Version
// bump.

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
		name          string
		n             int
		maxPartKeys   uint64
		groups        [][]int
		mph, mphSlots string
	}{
		// One partition, one-byte positions.
		{
			name: "parts=1", n: 200, groups: [][]int{{3, 77, 150}},
			mph:      "224e98a5834955b8346a83b5781bed9b5047005d3032d1792f386c9282fe015f",
			mphSlots: "029ac9e530ee1372f4c7d3b884fd31b5a3b55ae774c44ea154d94b4d8a9cd025",
		},
		// Two-byte positions.
		{
			name: "parts=1", n: 5000, groups: [][]int{{0, 4999}, {10, 20, 30, 40}},
			mph:      "c6242c819adba1310e125705aac688b1299f4f316cfb75573c7d00f183d7e7d2",
			mphSlots: "3e212a8e757f82b9a95356ea149a1745d24e6bea0d49862c3c40f2f6dc054c83",
		},
		// Partitions spanning several spill files.
		{
			name: "parts=64", n: 400_000, groups: [][]int{{7, 50_007, 100_007, 399_999}, {11, 99_999}},
			mph:      "c2836b8355ce10020d6fe653031eb72098765df97965bc9e957cfcfb5683a3d1",
			mphSlots: "9af88a77e57dad6caac5a2466989a20a2a86e0f90674f1d090a80154f3cd1229",
		},
		// Partitions split from the spill files.
		{
			name: "split", n: 400_000, maxPartKeys: 1000, groups: [][]int{{7, 50_007, 100_007, 399_999}, {11, 99_999}},
			mph:      "c9e122cccf37944464dea958925a1e089f225445d22872bda74835ef9d9d9c65",
			mphSlots: "508a84a763b85396966dae34d408bf1752b39b6f720e81c4b3c8d4d8b52cfafb",
		},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/n=%d", tc.name, tc.n), func(t *testing.T) {
			prefixes := make([]string, tc.n)
			for i := range prefixes {
				prefixes[i] = prefixFromInt(i + 1)
			}
			keyHash := goldenKeyHash(prefixes, tc.groups...)
			dir := t.TempDir()
			m, _ := mustBuildCase(t, dir, prefixes, mphfCase{partBits: -1, maxPartKeys: tc.maxPartKeys, workers: 4, keyHash: keyHash})
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

// TestBBHashFileMatchesGolden covers partitions deep enough for many
// levels, with keys equal on every level.
func TestBBHashFileMatchesGolden(t *testing.T) {
	keys := randomKeys(2_000_000, 42)
	keys = append(keys, keys[5], keys[5], keys[1_000_000])
	fps := make([]uint64, len(keys))
	for i, k := range keys {
		fps[i] = k ^ uint64(i)
	}
	const partBits = 5
	parts, err := testPartitions(t, keys, fps, partBits)
	if err != nil {
		t.Fatal(err)
	}
	var fallback int
	for _, p := range parts {
		fallback += len(p.fallback)
	}
	if fallback != 5 {
		t.Fatalf("%d fallback entries, want the 5 keys that share a hash", fallback)
	}
	const want = "46d013889e8496de41dd9317375d4e271f7940f007e8d3662a77561e044f977f"
	if sum := sha256.Sum256(encodeTable(t, partBits, parts)); hex.EncodeToString(sum[:]) != want {
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
