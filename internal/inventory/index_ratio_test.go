package inventory_test

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/dbtest"
	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// A ratio learned from a configuration's previous run replaces the
// format's conservative one, with a quarter added for the run-to-run
// drift measured on the seeded layouts.
func TestEstimateIndexBytes_Learned(t *testing.T) {
	const data = 1000
	if got := inventory.EstimateIndexBytes("CSV", data, inventory.IndexRatio{FileFormat: "CSV", Ratio: 1.5}); got != 1875 {
		t.Errorf("CSV learned 1.5x = %d, want 1875 (1.5 x 1.25 margin)", got)
	}
	if got := inventory.EstimateIndexBytes("parquet", data, inventory.IndexRatio{FileFormat: "Parquet", Ratio: 1.125}); got != 1406 {
		t.Errorf("Parquet learned 1.125x = %d, want 1406", got)
	}
	learned := inventory.IndexRatio{FileFormat: "CSV", Ratio: 2}
	if got := inventory.EstimateIndexBytes(inventory.IndexFileFormat, data, learned); got != data {
		t.Errorf("published index with a learned ratio = %d, want its size %d", got, data)
	}
}

// A configuration that switches format keeps its learned ratio,
// converted by how much larger Parquet files are than gzipped CSV.
func TestEstimateIndexBytes_FormatSwitch(t *testing.T) {
	const data = 1_000_000
	// Measured on the seeded realistic layout at 100K objects: the same
	// index is 1.418x its gzipped CSV and 1.095x its Parquet.
	fromCSV := inventory.EstimateIndexBytes("Parquet", data, inventory.IndexRatio{FileFormat: "CSV", Ratio: 1.418})
	if fromCSV < 1.095*data || fromCSV > 1.095*1.4*data {
		t.Errorf("CSV-learned estimate for Parquet = %d, want within [1.095, 1.53]x the data", fromCSV)
	}
	fromParquet := inventory.EstimateIndexBytes("CSV", data, inventory.IndexRatio{FileFormat: "Parquet", Ratio: 1.095})
	if fromParquet < 1.418*data || fromParquet > 1.418*1.4*data {
		t.Errorf("Parquet-learned estimate for CSV = %d, want within [1.418, 1.99]x the data", fromParquet)
	}
}

// A stored ratio that is zero, negative, not a number or absurdly
// large is held within sane bounds: a load neither reserves nothing nor
// is refused for a ratio no layout reaches.
func TestEstimateIndexBytes_ClampsLearned(t *testing.T) {
	const data = 1_000_000
	for _, format := range []string{"CSV", "Parquet"} {
		conservative := inventory.EstimateIndexBytes(format, data, inventory.IndexRatio{})
		for _, low := range []float64{0, -3, math.NaN()} {
			got := inventory.EstimateIndexBytes(format, data, inventory.IndexRatio{FileFormat: format, Ratio: low})
			if got == 0 || got > conservative/10 {
				t.Errorf("%s learned %v = %d, want above 0 and at most a tenth of the conservative %d", format, low, got, conservative)
			}
		}
		for _, high := range []float64{1e12, math.Inf(1)} {
			got := inventory.EstimateIndexBytes(format, data, inventory.IndexRatio{FileFormat: format, Ratio: high})
			if got < conservative || got > 2*conservative+1 { // +1: both round down
				t.Errorf("%s learned %v = %d, want within [1, 2]x the conservative %d", format, high, got, conservative)
			}
		}
	}
}

// A configuration's ratio is kept per configuration, replaced by its
// latest run, and absent before any.
func TestConfigStore_IndexRatio(t *testing.T) {
	ctx := t.Context()
	store := inventory.NewConfigStore(dbtest.OpenMemDB(t))
	if got, err := store.IndexRatio(ctx, "src", "inv"); err != nil || got != (inventory.IndexRatio{}) {
		t.Fatalf("IndexRatio before any run = %+v, %v; want none", got, err)
	}
	for _, r := range []inventory.IndexRatio{{FileFormat: "CSV", Ratio: 1.4}, {FileFormat: "Parquet", Ratio: 1.1}} {
		if err := store.RecordIndexRatio(ctx, "src", "inv", r); err != nil {
			t.Fatal(err)
		}
		if got, err := store.IndexRatio(ctx, "src", "inv"); err != nil || got != r {
			t.Errorf("IndexRatio = %+v, %v; want %+v", got, err, r)
		}
	}
	if got, err := store.IndexRatio(ctx, "src", "other"); err != nil || got != (inventory.IndexRatio{}) {
		t.Errorf("another configuration's IndexRatio = %+v, %v; want none", got, err)
	}
	if _, err := store.Get(ctx, "src", "inv"); err == nil {
		t.Error("recording a ratio created a configuration row")
	}
}

// learning is a Discovery whose gate loads every run, as the real gate
// does, recording each load's estimate.
type learning struct {
	*inventory.Discovery

	estimates []uint64
}

// learningDiscovery returns a learning Discovery whose builds return
// dir and which learns index ratios in store.
func learningDiscovery(t *testing.T, store *inventory.ConfigStore, dir string) *learning {
	t.Helper()
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	l := &learning{}
	gate := func(ctx context.Context, id inventory.ID, build inventory.BuildFunc, opts inventory.GatedLoadOptions) error {
		l.estimates = append(l.estimates, opts.EstimateBytes)

		return mgr.LoadWith(ctx, id, build)
	}
	l.Discovery = inventory.NewDiscovery(mgr,
		inventory.WithBackend(&fakeDiscoverer{bucket: "dst"}, &fakeBuilder{buildResp: dir}),
		inventory.WithGate(gate, nil),
		inventory.WithIndexRatios(store))

	return l
}

func csvRun(name, run string, dataBytes uint64) inventory.Inventory {
	return inventory.Inventory{SourceBucket: "b", Name: name, Run: run, ManifestKey: run, FileFormat: "CSV", TotalBytes: int64(dataBytes)}
}

// With no previous run a load reserves the conservative estimate; the
// next run of the configuration reserves its data times the previous
// run's index ratio, plus the margin.
func TestDiscovery_LoadEstimatesFromPreviousRun(t *testing.T) {
	dir := buildMinimalIndex(t)
	indexBytes, err := inventory.MeasureDir(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	store := inventory.NewConfigStore(dbtest.OpenMemDB(t))
	d := learningDiscovery(t, store, dir)

	published := inventory.Inventory{SourceBucket: "b", Name: "idx", Run: "2026-01-01T00-00Z", ManifestKey: "p", FileFormat: inventory.IndexFileFormat, TotalBytes: int64(indexBytes)}
	for _, disc := range []inventory.Inventory{
		csvRun("i", "2026-01-01T00-00Z", indexBytes),
		csvRun("i", "2026-01-02T00-00Z", 4*indexBytes),
		published,
	} {
		if err := d.Load(t.Context(), disc); err != nil {
			t.Fatalf("load %s: %v", disc.CompositeID(), err)
		}
	}
	want := []uint64{inventory.EstimateIndexBytes("CSV", indexBytes, inventory.IndexRatio{}), 5 * indexBytes, indexBytes}
	if !slices.Equal(d.estimates, want) {
		t.Errorf("estimates %v, want %v: conservative, then 4x the data at the learned 1.0x plus a quarter, then the published size", d.estimates, want)
	}
	if got, _ := store.IndexRatio(t.Context(), "b", "idx"); got != (inventory.IndexRatio{}) {
		t.Errorf("a published index recorded ratio %+v", got)
	}
}

// The learned ratio is persisted: after a restart, a run of the
// configuration, in the other format here, is still estimated from it.
func TestDiscovery_LearnedRatioSurvivesRestart(t *testing.T) {
	dir := buildMinimalIndex(t)
	indexBytes, err := inventory.MeasureDir(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	db := dbtest.OpenMemDB(t)
	before := learningDiscovery(t, inventory.NewConfigStore(db), dir)
	if err := before.Load(t.Context(), csvRun("i", "2026-01-01T00-00Z", 2*indexBytes)); err != nil {
		t.Fatal(err)
	}

	after := learningDiscovery(t, inventory.NewConfigStore(db), dir)
	next := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "2026-01-02T00-00Z", ManifestKey: "k", FileFormat: "Parquet", TotalBytes: int64(2 * indexBytes)}
	if err := after.Load(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	got := after.estimates[0]
	learned := inventory.IndexRatio{FileFormat: "CSV", Ratio: 0.5}
	if want := inventory.EstimateIndexBytes("Parquet", 2*indexBytes, learned); got != want {
		t.Errorf("estimate after restart = %d, want %d from the CSV run's 0.5x", got, want)
	}
	if conservative := inventory.EstimateIndexBytes("Parquet", 2*indexBytes, inventory.IndexRatio{}); got >= conservative {
		t.Errorf("estimate after restart = %d, not under the conservative %d", got, conservative)
	}
}
