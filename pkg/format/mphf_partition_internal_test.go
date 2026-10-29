package format

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
)

// mphfCase is how one test build runs.
type mphfCase struct {
	partBits    int    // fixed partition bits, or -1 to let the build choose
	maxPartKeys uint64 // the partition cap; 0 keeps maxPartitionKeys
	workers     int    // the workers the build starts with
	grow        int    // workers added once the build has started
	shuffle     uint64 // when nonzero, the keys are added in an order it seeds
	keyHash     func(string) uint64
}

// newCaseBuilder adds prefixes to a builder as c says, prefix i at
// position i.
func newCaseBuilder(t *testing.T, prefixes []string, c mphfCase) *StreamingMPHFBuilder {
	t.Helper()
	b := NewStreamingMPHFBuilder(t.TempDir())
	t.Cleanup(func() { b.Close() })
	b.partBits, b.keyHash = c.partBits, c.keyHash
	if c.maxPartKeys > 0 {
		b.maxPartKeys = c.maxPartKeys
	}
	order := make([]int, len(prefixes))
	for i := range order {
		order[i] = i
	}
	if c.shuffle != 0 {
		rand.New(rand.NewPCG(c.shuffle, 1)).Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	}
	for _, i := range order {
		keyHash, fp := b.KeyHashes(prefixes[i])
		if err := b.AddKey(keyHash, fp, uint64(i)); err != nil {
			t.Fatalf("AddKey(%q): %v", prefixes[i], err)
		}
	}

	return b
}

// buildCase builds the MPHF over prefixes into dir as c says.
func buildCase(t *testing.T, dir string, prefixes []string, c mphfCase) (*MPHFBuild, error) {
	t.Helper()
	b := newCaseBuilder(t, prefixes, c)
	h, err := b.Start(t.Context(), dir, nil, max(1, c.workers))
	if err != nil {
		return nil, err
	}
	if c.grow > 0 {
		h.Grow(c.grow)
	}

	err = h.Wait()

	return h, err
}

// mustBuildCase builds and opens the MPHF.
func mustBuildCase(t *testing.T, dir string, prefixes []string, c mphfCase) (*MPHF, *MPHFBuild) {
	t.Helper()
	h, err := buildCase(t, dir, prefixes, c)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	return m, h
}

func testPrefixes(n int) []string {
	prefixes := make([]string, n)
	for i := range prefixes {
		prefixes[i] = fmt.Sprintf("p/%d/", i)
	}

	return prefixes
}

// TestMPHFPartitionedLookupsExact: at every partition count, including
// partitions finer than the spill files (split from them), every
// prefix looks up to its position and non-members miss.
func TestMPHFPartitionedLookupsExact(t *testing.T) {
	cases := map[string]mphfCase{
		"parts=1":   {partBits: 0},
		"parts=4":   {partBits: 2},
		"parts=256": {partBits: 8},
		"auto":      {partBits: -1},
		"split":     {partBits: -1, maxPartKeys: 500},
	}
	nonMembers := make([]string, 100_000)
	r := rand.New(rand.NewPCG(9, 9))
	for i := range nonMembers {
		nonMembers[i] = fmt.Sprintf("q/%x/", r.Uint64())
	}
	for _, n := range []int{0, 1, 2, 1000, 70_000, 300_000} {
		prefixes := testPrefixes(n)
		for name, c := range cases {
			t.Run(fmt.Sprintf("n=%d/%s", n, name), func(t *testing.T) {
				c.workers = 4
				m, h := mustBuildCase(t, t.TempDir(), prefixes, c)
				if name == "split" && n == 300_000 && h.plan.bits <= spillBits {
					t.Fatalf("%d partition bits, want partitions split from the spill files", h.plan.bits)
				}
				if m.Count() != uint64(n) {
					t.Fatalf("Count = %d, want %d", m.Count(), n)
				}
				for i, p := range prefixes {
					if pos, ok := m.Lookup(p); !ok || pos != uint64(i) {
						t.Fatalf("Lookup(%q) = %d, %t; want %d", p, pos, ok, i)
					}
				}
				for _, p := range nonMembers {
					if pos, ok := m.Lookup(p); ok {
						t.Fatalf("Lookup(%q) = %d for a non-member", p, pos)
					}
				}
			})
		}
	}
}

// TestMPHFPositionKeys: prefixes added in any order with their depth
// and index within it look up to their depth's first position plus the
// index.
func TestMPHFPositionKeys(t *testing.T) {
	depthSizes := []int{1, 30, 900, 20_000, 5}
	starts := make([]uint64, len(depthSizes)+1)
	type key struct {
		prefix       string
		depth, local int
	}
	var keys []key
	for d, size := range depthSizes {
		starts[d+1] = starts[d] + uint64(size)
		for i := range size {
			keys = append(keys, key{fmt.Sprintf("d%d/%d/", d, i), d, i})
		}
	}
	rand.New(rand.NewPCG(3, 3)).Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	b := NewStreamingMPHFBuilder(t.TempDir())
	defer b.Close()
	for _, k := range keys {
		posKey, err := PositionKey(k.depth, uint64(k.local))
		if err != nil {
			t.Fatal(err)
		}
		keyHash, fp := b.KeyHashes(k.prefix)
		if err := b.AddKey(keyHash, fp, posKey); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	h, err := b.Start(t.Context(), dir, starts, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Wait(); err != nil {
		t.Fatal(err)
	}
	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, k := range keys {
		want := starts[k.depth] + uint64(k.local)
		if pos, ok := m.Lookup(k.prefix); !ok || pos != want {
			t.Fatalf("Lookup(%q) = %d, %t; want %d", k.prefix, pos, ok, want)
		}
	}
}

func TestPositionKeyRange(t *testing.T) {
	for _, c := range []struct {
		depth int
		local uint64
		ok    bool
	}{
		{0, 0, true},
		{posMaxDepth, posLocalMask, true},
		{posMaxDepth + 1, 0, false},
		{-1, 0, false},
		{0, posLocalMask + 1, false},
	} {
		key, err := PositionKey(c.depth, c.local)
		if c.ok != (err == nil) {
			t.Errorf("PositionKey(%d, %d) = %v, want ok %t", c.depth, c.local, err, c.ok)
		}
		if err != nil && !errors.Is(err, ErrMPHFPosition) {
			t.Errorf("PositionKey(%d, %d) = %v, want ErrMPHFPosition", c.depth, c.local, err)
		}
		if c.ok && (key>>posLocalBits != uint64(c.depth) || key&posLocalMask != c.local) {
			t.Errorf("PositionKey(%d, %d) = %#x", c.depth, c.local, key)
		}
	}
}

// TestMPHFPositionsOnePerKey: a build fails rather than map two keys to
// one position, or a key past its depth or the key count, and leaves no
// output.
func TestMPHFPositionsOnePerKey(t *testing.T) {
	const n = 5000
	cases := map[string]struct {
		starts []uint64
		posKey func(i int) uint64
	}{
		"duplicate":   {posKey: func(i int) uint64 { return uint64(min(i, n-2)) }},
		"past_count":  {posKey: func(i int) uint64 { return uint64(i + 1) }},
		"past_depth":  {starts: []uint64{0, 1, n}, posKey: func(i int) uint64 { return uint64(i) }},
		"no_depth":    {starts: []uint64{0, n}, posKey: func(i int) uint64 { return 1<<posLocalBits | uint64(i) }},
		"short_start": {starts: []uint64{0, n - 1}, posKey: func(i int) uint64 { return uint64(i) }},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			b := NewStreamingMPHFBuilder(t.TempDir())
			defer b.Close()
			for i := range n {
				keyHash, fp := b.KeyHashes(fmt.Sprintf("k%d/", i))
				if err := b.AddKey(keyHash, fp, c.posKey(i)); err != nil {
					t.Fatal(err)
				}
			}
			dir := t.TempDir()
			h, err := b.Start(t.Context(), dir, c.starts, 2)
			if err == nil {
				err = h.Wait()
			}
			if !errors.Is(err, ErrMPHFPosition) {
				t.Fatalf("build = %v, want ErrMPHFPosition", err)
			}
			assertEmptyDir(t, dir)
		})
	}
}

// assertEmptyDir fails if dir holds anything.
func assertEmptyDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("%s holds %v, want nothing", dir, names)
	}
}

// buildDigests builds into a fresh dir and returns the SHA-256 of
// mph.bin and mph_slots.bin.
func buildDigests(t *testing.T, prefixes []string, c mphfCase) [2]string {
	t.Helper()
	dir := t.TempDir()
	if _, err := buildCase(t, dir, prefixes, c); err != nil {
		t.Fatal(err)
	}

	return [2]string{fileDigest(t, filepath.Join(dir, MPHFile)), fileDigest(t, filepath.Join(dir, MPHFSlotsFile))}
}

// TestMPHFBuildIsDeterministic: the files depend only on the keys, not
// on the worker count, workers added mid-build, or the order the keys
// were added in.
func TestMPHFBuildIsDeterministic(t *testing.T) {
	prefixes := testPrefixes(300_000)
	for name, base := range map[string]mphfCase{
		"auto":  {partBits: -1},
		"split": {partBits: -1, maxPartKeys: 500},
	} {
		t.Run(name, func(t *testing.T) {
			ref := buildDigests(t, prefixes, mphfCase{partBits: base.partBits, maxPartKeys: base.maxPartKeys, workers: 1})
			for _, c := range []mphfCase{
				{workers: 2},
				{workers: 8},
				{workers: 1, grow: 7},
				{workers: 3, shuffle: 11},
			} {
				c.partBits, c.maxPartKeys = base.partBits, base.maxPartKeys
				if got := buildDigests(t, prefixes, c); got != ref {
					t.Errorf("workers %d, grow %d, shuffle %d: digests %v, want %v", c.workers, c.grow, c.shuffle, got, ref)
				}
			}
		})
	}
}

// TestMPHFFallbackPerPartition: keys that share a hash become fallback
// entries of their own partition, and every lookup still resolves. A
// key hash that puts every key in one partition still builds exactly,
// at any partition count.
func TestMPHFFallbackPerPartition(t *testing.T) {
	prefixes := testPrefixes(70_000)
	t.Run("groups", func(t *testing.T) {
		keyHash := goldenKeyHash(prefixes, []int{1, 2, 3}, []int{40_000, 50_000}, []int{7, 69_999, 12, 13})
		m, h := mustBuildCase(t, t.TempDir(), prefixes, mphfCase{partBits: 8, workers: 4, keyHash: keyHash})
		if len(m.mph.fallback) != 9 {
			t.Fatalf("%d fallback entries, want 9", len(m.mph.fallback))
		}
		holding := 0
		for _, p := range m.mph.parts {
			if p.fbEnd > p.fbStart {
				holding++
			}
		}
		if holding != 3 || h.plan.bits != 8 {
			t.Errorf("%d of %d partitions hold fallback entries, want 3", holding, len(m.mph.parts))
		}
		assertLookups(t, m, keyHash, prefixes)
	})
	t.Run("one_partition", func(t *testing.T) {
		keyHash := func(s string) uint64 { return hashString(s) >> 20 }
		for _, maxKeys := range []uint64{0, 1000} {
			m, h := mustBuildCase(t, t.TempDir(), prefixes, mphfCase{partBits: -1, maxPartKeys: maxKeys, workers: 4, keyHash: keyHash})
			if h.plan.maxPart != uint64(len(prefixes)) {
				t.Errorf("largest partition %d keys, want all %d", h.plan.maxPart, len(prefixes))
			}
			assertLookups(t, m, keyHash, prefixes)
		}
	})
	t.Run("full_collision", func(t *testing.T) {
		b := newCaseBuilder(t, prefixes[:100], mphfCase{partBits: 2, keyHash: collidingKeyHash(prefixes[5], prefixes[6])})
		keyHash, fp := b.KeyHashes(prefixes[5])
		if err := b.AddKey(keyHash, fp, 100); err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		h, err := b.Start(t.Context(), dir, nil, 2)
		if err == nil {
			err = h.Wait()
		}
		if !errors.Is(err, ErrMPHFKeyCollision) {
			t.Fatalf("build = %v, want ErrMPHFKeyCollision", err)
		}
		assertEmptyDir(t, dir)
	})
}

// TestMPHFBuildStops: a build aborted or cancelled at any point,
// including before it starts, ends with its error and leaves neither
// spill files nor output; Close after it is clean.
func TestMPHFBuildStops(t *testing.T) {
	prefixes := testPrefixes(300_000)
	for _, how := range []string{"abort", "cancel", "cancelled_before", "close"} {
		for _, maxKeys := range []uint64{0, 500} {
			t.Run(fmt.Sprintf("%s/maxPartKeys=%d", how, maxKeys), func(t *testing.T) {
				tmp, dir := t.TempDir(), t.TempDir()
				b := NewStreamingMPHFBuilder(tmp)
				if maxKeys > 0 {
					b.maxPartKeys = maxKeys
				}
				for _, p := range prefixes {
					if err := b.Add(p); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if how == "cancelled_before" {
					cancel()
				}
				h, err := b.Start(ctx, dir, nil, 2)
				if err != nil {
					t.Fatal(err)
				}
				var wg sync.WaitGroup
				wg.Go(func() { h.Grow(2) })
				want := context.Canceled
				switch how {
				case "abort":
					h.Abort()
					want = ErrMPHFAborted
				case "cancel":
					cancel()
				case "close":
					want = ErrMPHFAborted
					if err := b.Close(); err != nil {
						t.Fatalf("Close: %v", err)
					}
				}
				wg.Wait()
				if err := h.Wait(); !errors.Is(err, want) {
					t.Fatalf("Wait = %v, want %v", err, want)
				}
				if err := b.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				assertEmptyDir(t, tmp)
				assertEmptyDir(t, dir)
			})
		}
	}
}

// TestMPHFBuildKeepsFilesItDidNotCreate: a build that cannot create its
// output fails, and removes nothing it did not create.
func TestMPHFBuildKeepsFilesItDidNotCreate(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, MPHFSlotsFile)
	if err := os.Mkdir(blocker, 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := buildCase(t, dir, testPrefixes(1000), mphfCase{partBits: -1}); err == nil {
		t.Fatal("build succeeded with a directory in the slot file's place")
	}
	if info, err := os.Stat(blocker); err != nil || !info.IsDir() {
		t.Fatalf("the directory in the slot file's place is gone: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, MPHFile)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the build left %s: %v", MPHFile, err)
	}
}

// TestMPHFBuildWorkersFitBudget: the workers a build runs, however many
// it is asked for, fit the memory budget given its largest partition.
func TestMPHFBuildWorkersFitBudget(t *testing.T) {
	prefixes := testPrefixes(300_000)
	for _, budgetWorkers := range []int{1, 3} {
		b := newCaseBuilder(t, prefixes, mphfCase{partBits: -1})
		plan := planPartitions(b.spill.hist, b.count, -1, maxPartitionKeys)
		perWorker := int64(plan.maxPart)*int64(partKeyBytes+plan.slotW) + spillReadBytes
		b.SetMemoryBudget(int64(budgetWorkers)*perWorker + perWorker/2)
		h, err := b.Start(t.Context(), t.TempDir(), nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		h.Grow(100)
		if err := h.Wait(); err != nil {
			t.Fatal(err)
		}
		want := min(budgetWorkers, runtime.GOMAXPROCS(0))
		if h.maxWorkers != want || h.running > want {
			t.Errorf("budget for %d workers: up to %d, ran %d", budgetWorkers, h.maxWorkers, h.running)
		}
	}
}

// TestMPHFBuildHeapBounded: with one worker and a small partition cap,
// the build's heap stays near the cap's worth of keys whatever the key
// count: twice the keys take about the same peak heap.
func TestMPHFBuildHeapBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("builds 3M keys")
	}
	const maxKeys = 1 << 14
	peak := func(n int) uint64 {
		b := NewStreamingMPHFBuilder(t.TempDir())
		defer b.Close()
		b.maxPartKeys = maxKeys
		b.SetMemoryBudget(1)
		r := rand.New(rand.NewPCG(uint64(n), 2))
		for i := range n {
			if err := b.AddKey(r.Uint64(), r.Uint64(), uint64(i)); err != nil {
				t.Fatal(err)
			}
		}
		runtime.GC()
		var base runtime.MemStats
		runtime.ReadMemStats(&base)
		sampler := benchutil.StartHeapPeakSampler()
		h, err := b.Start(t.Context(), t.TempDir(), nil, 4)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.Wait(); err != nil {
			t.Fatal(err)
		}
		top := sampler.Stop()
		if h.maxWorkers != 1 || h.plan.maxPart > maxKeys {
			t.Fatalf("%d workers, largest partition %d keys; want 1 worker and at most %d", h.maxWorkers, h.plan.maxPart, maxKeys)
		}

		return top - min(top, base.HeapAlloc)
	}
	small, large := peak(1_000_000), peak(2_000_000)
	const slack = 3 << 20
	bound := uint64(maxKeys*(partKeyBytes+mphfFingerprintBytes+3)) + slack
	if small > bound || large > bound {
		t.Errorf("peak heap %d at 1M keys and %d at 2M, want at most %d", small, large, bound)
	}
	if large > small*12/10+slack/8 {
		t.Errorf("peak heap grows with the keys: %d at 1M, %d at 2M", small, large)
	}
}

// TestMPHFBuildReportsProgress: the build counts its keys as one step,
// from progress set after it started, up to the key count.
func TestMPHFBuildReportsProgress(t *testing.T) {
	b := newCaseBuilder(t, testPrefixes(100_000), mphfCase{partBits: 4})
	h, err := b.Start(t.Context(), t.TempDir(), nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var steps []string
	var last int64
	h.SetProgress(func(step string, done, total int64) {
		mu.Lock()
		defer mu.Unlock()
		if total != 100_000 || done < last {
			t.Errorf("progress %d of %d after %d", done, total, last)
		}
		steps, last = append(steps, step), done
	})
	if err := h.Wait(); err != nil {
		t.Fatal(err)
	}
	if steps = slices.Compact(steps); !slices.Equal(steps, []string{StepBuildHash}) || last != 100_000 {
		t.Errorf("steps %q ending at %d, want %q ending at 100000", steps, last, StepBuildHash)
	}
}

// TestChoosePartBits: about 2^13 keys a partition up to the spill
// files' count, finer past it only while a partition holds too many.
func TestChoosePartBits(t *testing.T) {
	uniform := func(n uint64) []uint64 {
		hist := make([]uint64, 1<<maxPartBits)
		for i := range hist {
			hist[i] = n >> maxPartBits
		}

		return hist
	}
	for _, c := range []struct {
		n, maxKeys uint64
		want       uint
	}{
		{1, maxPartitionKeys, 0},
		{1 << 13, maxPartitionKeys, 0},
		{1<<13 + 1, maxPartitionKeys, 1},
		{1_000_000, maxPartitionKeys, 7},
		{10_000_000, maxPartitionKeys, 8},
		{1 << 28, maxPartitionKeys, 8},
		{1 << 30, maxPartitionKeys, 10},
		{1 << 40, maxPartitionKeys, maxPartBits},
		{1 << 24, 1 << 10, 14},
	} {
		if got := choosePartBits(uniform(c.n), c.n, c.maxKeys); got != c.want {
			t.Errorf("choosePartBits(n=%d, max %d) = %d, want %d", c.n, c.maxKeys, got, c.want)
		}
	}
}

// TestMPHFBuildAllocsIndependentOfKeys: Add and the build allocate per
// spill file, partition and worker, never per key: once every bucket
// has a spill file, eight times the keys over the same partitions and
// workers allocate about as often. A build whose buckets fit in their
// buffers creates no spill file at all.
func TestMPHFBuildAllocsIndependentOfKeys(t *testing.T) {
	const partBits, workers = 6, 4
	allocs := func(n, wantFiles int) uint64 {
		b := NewStreamingMPHFBuilder(t.TempDir())
		defer b.Close()
		b.partBits = partBits
		dir := t.TempDir()
		r := rand.New(rand.NewPCG(uint64(n), 5))
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := range n {
			if err := b.AddKey(r.Uint64(), r.Uint64(), uint64(i)); err != nil {
				t.Fatal(err)
			}
		}
		files := 0
		for i := range b.spill.files {
			if b.spill.files[i].f != nil {
				files++
			}
		}
		if wantFiles >= 0 && files != wantFiles {
			t.Fatalf("%d keys made %d spill files, want %d", n, files, wantFiles)
		}
		h, err := b.Start(context.Background(), dir, nil, workers)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.Wait(); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		if h.running != workers {
			t.Fatalf("%d workers ran, want %d", h.running, workers)
		}

		return after.Mallocs - before.Mallocs
	}
	limit := uint64(8*spillBuckets + 16*(1<<partBits) + 32*workers + 512)
	if got := allocs(100_000, 0); got > limit/4 {
		t.Errorf("%d allocations at 100K keys, without spill files, want at most %d", got, limit/4)
	}
	small, large := allocs(200_000, -1), allocs(1_600_000, spillBuckets)
	if small > limit || large > limit {
		t.Errorf("%d allocations at 200K keys and %d at 1.6M, want at most %d", small, large, limit)
	}
	if large > small+small/4 {
		t.Errorf("allocations grow with the keys: %d at 200K, %d at 1.6M", small, large)
	}
}
