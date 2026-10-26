package templates_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/templates"
)

// namedID mirrors inventory.ID: a string kind with a String method and
// no marshaler.
type namedID string

func (id namedID) String() string { return string(id) }

// BenchmarkHxValsJSON times the hx-vals encoder templates call.
func BenchmarkHxValsJSON(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := templates.HxValsJSON("inventory_id", namedID("src/inv/2026-01-01T00-00Z"), "prefix", "data/2024/c000123/"); err != nil {
			b.Fatal(err)
		}
	}
}
