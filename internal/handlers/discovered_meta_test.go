package handlers_test

import (
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
)

func TestDiscoveredMetaLine(t *testing.T) {
	tests := []struct {
		name string
		view handlers.DiscoveredRowView
		want string
	}{
		{
			name: "all four fields joined when loaded",
			view: handlers.DiscoveredRowView{
				MergedInventory: inventory.MergedInventory{
					Inventory: inventory.Inventory{FileFormat: "csv", FileCount: 24, TotalBytes: 1_500_000_000},
					State:     inventory.StateLoaded,
				},
				CacheBytesH:   "47 MiB",
				LoadDurationH: "32.00s",
			},
			want: "csv · manifest 1.40 GiB / 24 chunks · cache 47 MiB · loaded in 32.00s",
		},
		{
			name: "singular chunk, no bytes, no cache",
			view: handlers.DiscoveredRowView{
				MergedInventory: inventory.MergedInventory{
					Inventory: inventory.Inventory{FileFormat: "parquet", FileCount: 1},
					State:     inventory.StateNotLoaded,
				},
			},
			want: "parquet · manifest 1 chunk",
		},
		{
			name: "cache+loaded-in suppressed when state is not loaded",
			view: handlers.DiscoveredRowView{
				MergedInventory: inventory.MergedInventory{
					State: inventory.StateLoading,
				},
				CacheBytesH:   "12 MiB",
				LoadDurationH: "5.00s",
			},
			want: "",
		},
		{
			name: "cache present and loaded",
			view: handlers.DiscoveredRowView{
				MergedInventory: inventory.MergedInventory{
					State: inventory.StateLoaded,
				},
				CacheBytesH: "12 MiB",
			},
			want: "cache 12 MiB",
		},
		{
			name: "nothing yields empty string",
			view: handlers.DiscoveredRowView{},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := handlers.DiscoveredMetaLineForTest(&tt.view); got != tt.want {
				t.Errorf("discoveredMetaLine = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestDiscoveredMetaLine_StateGuard pins the regression for the
// "stale loaded in" bug: the LoadDuration field can carry the previous
// successful load's value into StateLoading / StateNotLoaded / StateError,
// so the segments that describe an actively-loaded run must be suppressed
// unless the run is currently loaded.
func TestDiscoveredMetaLine_StateGuard(t *testing.T) {
	base := handlers.DiscoveredRowView{
		MergedInventory: inventory.MergedInventory{
			Inventory: inventory.Inventory{FileFormat: "csv", FileCount: 24, TotalBytes: 1_500_000_000},
		},
		CacheBytesH:   "47 MiB",
		LoadDurationH: "32.00s",
	}

	tests := []struct {
		name            string
		state           inventory.State
		wantHasLoadedIn bool
		wantHasCache    bool
		wantHasManifest bool
	}{
		{"loaded shows everything", inventory.StateLoaded, true, true, true},
		{"loading hides cache + loaded-in", inventory.StateLoading, false, false, true},
		{"not_loaded hides cache + loaded-in", inventory.StateNotLoaded, false, false, true},
		{"error hides cache + loaded-in", inventory.StateError, false, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := base
			v.State = tt.state
			got := handlers.DiscoveredMetaLineForTest(&v)
			if has := strings.Contains(got, "loaded in"); has != tt.wantHasLoadedIn {
				t.Errorf("loaded-in segment present=%v want=%v in %q", has, tt.wantHasLoadedIn, got)
			}
			if has := strings.Contains(got, "cache "); has != tt.wantHasCache {
				t.Errorf("cache segment present=%v want=%v in %q", has, tt.wantHasCache, got)
			}
			if has := strings.Contains(got, "manifest "); has != tt.wantHasManifest {
				t.Errorf("manifest segment present=%v want=%v in %q", has, tt.wantHasManifest, got)
			}
		})
	}
}
