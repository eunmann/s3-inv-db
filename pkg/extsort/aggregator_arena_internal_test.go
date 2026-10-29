package extsort

import (
	"errors"
	"io"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
	gocmp "github.com/google/go-cmp/cmp"
)

// Segments longer than an eighth of a key chunk get chunks of their own,
// even one longer than a whole chunk, beside short keys filling shared
// chunks; through the node table, an ordered segment and its mapping,
// Drain and a run written from the nodes both add up to a direct count,
// and a spill reuses the chunks.
func TestAggregatorLongSegments(t *testing.T) {
	long := []string{
		strings.Repeat("a", segmentArenaChunk/8-nodeKeyHeader-1) + "/",
		strings.Repeat("b", segmentArenaChunk/8) + "/",
		strings.Repeat("c", segmentArenaChunk+100) + "/",
		strings.Repeat("c", segmentArenaChunk+100) + "x/",
	}
	rng := rand.New(rand.NewPCG(9, 10))
	keys := pathTestKeys(rng, 4000)
	for i := range keys {
		if i%50 == 0 {
			keys[i] = long[rng.IntN(len(long))] + keys[i]
		}
	}
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	tierOf := func(i int) tiers.ID { return tiers.ID(i % MaxTiers) }
	for name, order := range map[string][]string{"random": keys, "sorted": sorted} {
		want := referenceAggregateTiers(order, 0, tierOf)
		for _, viaRun := range []bool{false, true} {
			agg := NewAggregator(0, 0)
			got := map[string]PrefixRow{}
			for round := range 2 {
				for i, k := range order {
					if i%1000 == 0 {
						agg.StartSegment()
					}
					agg.AddObject(k, uint64(len(k)+i), tierOf(i))
				}
				checkAggregatorAccounting(t, agg)
				chunks := agg.segs.inUse
				if round == 0 {
					var rows []*PrefixRow
					if viaRun {
						rows = memRunRows(t, agg)
					} else {
						rows = agg.Drain()
					}
					mergeRows(got, rows)
					if !maps.Equal(want, got) {
						t.Fatalf("%s viaRun=%v (-reference +aggregator):\n%s", name, viaRun, gocmp.Diff(want, got))
					}
					continue
				}
				if held := len(agg.segs.chunks); held != chunks {
					t.Errorf("%s: the same keys again take %d chunks of %d held", name, chunks, held)
				}
			}
		}
	}
}

// memRunRows writes agg's rows as a memory run and reads them back.
func memRunRows(t *testing.T, agg *Aggregator) []*PrefixRow {
	t.Helper()
	w := newMemRunWriter()
	if _, err := agg.writeRun(&w.enc); err != nil {
		t.Fatalf("writeRun: %v", err)
	}
	run := w.finish()
	var rows []*PrefixRow
	for {
		row := &PrefixRow{}
		_, err := run.readNext(row)
		if errors.Is(err, io.EOF) {
			return rows
		}
		if err != nil {
			t.Fatalf("read memory run: %v", err)
		}
		rows = append(rows, row)
	}
}

// Full reports the aggregator full while one more key of any length S3
// allows still fits under both 32-bit limits, nodes and key chunks.
func TestAggregatorFullLeavesRoomForAKey(t *testing.T) {
	const maxKey = 1024
	if aggNodeLimit+maxKey+1 >= deadNode {
		t.Errorf("Full at %d nodes leaves no room for a key's %d below %d", aggNodeLimit, maxKey+1, deadNode)
	}
	// A key of slashes alone has the most segments; each takes a header.
	if keyBytes := maxKey * (nodeKeyHeader + 1); arenaChunkLimit+keyBytes/segmentArenaChunk+1 >= maxArenaChunks {
		t.Errorf("Full at %d chunks leaves no room for a key's %d bytes", arenaChunkLimit, keyBytes)
	}
	agg := NewAggregator(0, 0)
	if agg.Full() {
		t.Fatal("a new aggregator is full")
	}
	agg.used = aggNodeLimit
	if !agg.Full() {
		t.Errorf("not full at %d nodes", agg.used)
	}
	agg.used, agg.segs.inUse = 0, arenaChunkLimit
	if !agg.Full() {
		t.Errorf("not full at %d key chunks", agg.segs.inUse)
	}
}
