package benchutil_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
)

func TestGenerator_SeededOutputIsDeterministic(t *testing.T) {
	a := benchutil.NewGenerator(benchutil.S3RealisticConfig(20_000)).Generate()
	b := benchutil.NewGenerator(benchutil.S3RealisticConfig(20_000)).Generate()
	if len(a) != len(b) {
		t.Fatalf("lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("object %d differs between runs with the same seed: %+v vs %+v", i, a[i], b[i])
		}
	}
}
