package handlers_test

import (
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
			name: "all four fields joined",
			view: handlers.DiscoveredRowView{
				MergedInventory: inventory.MergedInventory{
					Inventory: inventory.Inventory{FileFormat: "csv", FileCount: 24, TotalBytes: 1_500_000_000},
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
				},
			},
			want: "parquet · manifest 1 chunk",
		},
		{
			name: "only cache present",
			view: handlers.DiscoveredRowView{
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
