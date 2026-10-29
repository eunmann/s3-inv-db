package benchutil_test

import (
	"strings"
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

func TestGenerator_UniqueDirsGivesEachObjectItsOwnDirectory(t *testing.T) {
	const n = 20_000
	objects := benchutil.NewGenerator(benchutil.S3UniqueDirsConfig(n)).Generate()
	dirs := make(map[string]struct{}, n)
	for _, o := range objects {
		parts := strings.Split(o.Key, "/")
		if len(parts) != 4 || parts[0] != "uploads" || !strings.HasPrefix(parts[1], "user_") {
			t.Fatalf("key %q is not uploads/user_N/<uuid>/<file>", o.Key)
		}
		dirs[strings.Join(parts[:3], "/")] = struct{}{}
	}
	if len(dirs) != n {
		t.Errorf("%d objects share %d directories, want one each", n, len(dirs))
	}
}
