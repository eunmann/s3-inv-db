package templates_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/templates"
)

// namedID mirrors inventory.ID: a string kind with a String method and
// no marshaler.
type namedID string

func (id namedID) String() string { return string(id) }

// The browse and compare tables call these once or twice per row.

func BenchmarkHxValsJSON(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := templates.HxValsJSON("inventory_id", namedID("src/inv/2026-01-01T00-00Z"), "prefix", "data/2024/c000123/"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBrowseURL(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := templates.BrowseURL("inventory_id", namedID("src/inv/2026-01-01T00-00Z"), "prefix", "data/2024/c000123/"); err != nil {
			b.Fatal(err)
		}
	}
}
