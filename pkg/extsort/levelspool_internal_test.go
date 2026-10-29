package extsort

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// spoolTestRecord is the i-th record of the spool tests, at depth i%4.
func spoolTestRecord(i int) spoolRecord {
	r := spoolRecord{
		count: uint64(i), bytes: uint64(i) << 20, childLocal: uint64(i / 2), parentLocal: uint64(i / 3),
		keyHash: uint64(i) * 0x9e3779b97f4a7c15, fp: ^uint64(i), name: fmt.Appendf(nil, "seg-%d", i),
	}
	r.tierCounts[i%MaxTiers], r.tierBytes[i%MaxTiers] = uint64(i), uint64(i)<<20

	return r
}

// TestLevelSpoolWritesEveryRecordInOrder: records handed over across
// several batches, and a last partial one, read back per depth in the
// order they were added.
func TestLevelSpoolWritesEveryRecordInOrder(t *testing.T) {
	const n = 3*spoolBatchRows + 5
	s := newLevelSpool(t.TempDir())
	t.Cleanup(func() { _ = s.close() })
	for i := range n {
		e := s.next()
		e.depth, e.rec = i%4, spoolTestRecord(i)
		if err := s.commit(); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	for depth := range 4 {
		i := depth
		err := s.read(depth, func(r *spoolRecord) error {
			if diff := cmp.Diff(spoolTestRecord(i), *r, cmp.AllowUnexported(spoolRecord{})); diff != "" {
				t.Errorf("record %d (-added +read):\n%s", i, diff)
			}
			i += 4

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if i < n {
			t.Fatalf("depth %d read up to record %d of %d", depth, i, n)
		}
	}
}

// TestLevelSpoolWriterErrorSurfaces: a write the writer goroutine fails
// reaches the builder through commit, then finish, and nothing hangs.
func TestLevelSpoolWriterErrorSurfaces(t *testing.T) {
	s := newLevelSpool(filepath.Join(t.TempDir(), "missing"))
	var commitErr error
	for i := 0; commitErr == nil && i < 10*spoolBatchRows; i++ {
		e := s.next()
		e.depth, e.rec = 0, spoolTestRecord(i)
		commitErr = s.commit()
	}
	if commitErr == nil {
		t.Fatal("commit never returned the writer's error")
	}
	if err := s.finish(); !errors.Is(err, commitErr) {
		t.Fatalf("finish = %v, want the commit error %v", err, commitErr)
	}
	if err := s.close(); !errors.Is(err, commitErr) {
		t.Fatalf("close = %v, want the commit error %v", err, commitErr)
	}
}

// TestLevelSpoolCommitAfterFinish: a row added after finish is refused,
// not lost or a panic.
func TestLevelSpoolCommitAfterFinish(t *testing.T) {
	for _, rows := range []int{0, 1} {
		s := newLevelSpool(t.TempDir())
		for i := range rows {
			e := s.next()
			e.rec = spoolTestRecord(i)
			if err := s.commit(); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.finish(); err != nil {
			t.Fatal(err)
		}
		s.next().rec = spoolTestRecord(rows)
		if err := s.commit(); !errors.Is(err, errSpoolStopped) {
			t.Fatalf("after %d rows: commit after finish = %v, want errSpoolStopped", rows, err)
		}
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
	}
}
