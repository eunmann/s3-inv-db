package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unsafe"
)

// treeTestRow is a prefix in position order: its parent's position
// (-1 for the root) and its last segment.
type treeTestRow struct {
	prefix string
	parent int
	name   string
}

// preorderRows gives prefixes (root first, each after its parent, in
// preorder) their positions in list order.
func preorderRows(t *testing.T, prefixes []string) []treeTestRow {
	t.Helper()
	rows := make([]treeTestRow, len(prefixes))
	var stack []int
	for i, p := range prefixes {
		for len(stack) > 0 && !strings.HasPrefix(p, prefixes[stack[len(stack)-1]]) {
			stack = stack[:len(stack)-1]
		}
		rows[i] = treeTestRow{prefix: p, parent: -1}
		if len(stack) > 0 {
			parent := stack[len(stack)-1]
			rows[i].parent = parent
			rows[i].name = strings.TrimSuffix(strings.TrimPrefix(p, prefixes[parent]), "/")
		} else if i != 0 {
			t.Fatalf("%q has no parent before it", p)
		}
		stack = append(stack, i)
	}

	return rows
}

// writeTree writes a prefix tree for prefixes (in preorder, root first,
// each after its parent) into a fresh directory.
func writeTree(t *testing.T, prefixes []string) string {
	t.Helper()
	dir := t.TempDir()
	writeTreeRows(t, dir, preorderRows(t, prefixes), nil)

	return dir
}

// writeTreeRows writes rows into dir, interning their segments in the
// order internOrder lists the rows (position order when nil).
func writeTreeRows(t *testing.T, dir string, rows []treeTestRow, internOrder []int) {
	t.Helper()
	ids, segments, maxDist := internTreeRows(t, dir, rows, internOrder)
	w, err := NewPrefixTreeWriter(dir, uint64(len(rows)), segments, maxDist, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rows {
		if r.parent < 0 {
			err = w.AddRoot()
		} else {
			err = w.Add(uint64(r.parent), ids[i])
		}
		if err != nil {
			t.Fatalf("row %d (%q): %v", i, r.prefix, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// internTreeRows writes the dictionary of rows' segments into dir,
// interned in the order internOrder lists the rows (position order when
// nil), and returns each row's segment ID, the segment count and the
// largest parent distance.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func internTreeRows(t *testing.T, dir string, rows []treeTestRow, internOrder []int) ([]uint32, uint32, uint64) {
	t.Helper()
	interner, err := NewPrefixSegmentInterner(dir, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if internOrder == nil {
		internOrder = make([]int, len(rows))
		for i := range internOrder {
			internOrder[i] = i
		}
	}
	ids := make([]uint32, len(rows))
	var maxDist uint64
	for _, i := range internOrder {
		if rows[i].parent < 0 {
			continue
		}
		if ids[i], err = interner.Intern(rows[i].name); err != nil {
			t.Fatal(err)
		}
		maxDist = max(maxDist, uint64(i-rows[i].parent))
	}
	if err := interner.Close(); err != nil {
		t.Fatal(err)
	}

	return ids, interner.Count(), maxDist
}

// levelOrderTreeRows is a random tree in level order: names repeat
// across parents, as dates and IDs do in real keys.
func levelOrderTreeRows(r *rand.Rand, depths, fanout, names int) []treeTestRow {
	rows := make([]treeTestRow, 1, 1<<10)
	rows[0].parent = -1
	level := []int{0}
	for range depths {
		next := make([]int, 0, len(level)*fanout)
		for _, parent := range level {
			picked := map[string]bool{}
			for range r.IntN(fanout + 1) {
				picked[fmt.Sprintf("n%d", r.IntN(names))] = true
			}
			for _, name := range slices.Sorted(maps.Keys(picked)) {
				next = append(next, len(rows))
				rows = append(rows, treeTestRow{prefix: rows[parent].prefix + name + "/", parent: parent, name: name})
			}
		}
		level = next
	}

	return rows
}

func openTree(t *testing.T, dir string) *PrefixTreeReader {
	t.Helper()
	r, err := OpenPrefixTreeReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })

	return r
}

func TestPrefixTreeRoundTrip(t *testing.T) {
	deep := ""
	prefixes := make([]string, 1, 113)
	for i := range 100 { // deeper than Prefix's on-stack ID buffer
		deep += fmt.Sprintf("d%d/", i)
		prefixes = append(prefixes, deep)
	}
	prefixes = append(prefixes,
		"/", "//", // empty segments
		"a/", "a//", "a/b/", "a/b/c/", "a/x/",
		"ab/",
		"emoji/", "emoji/🎉/",
		"日本語/", "日本語/パス/",
	)
	r := openTree(t, writeTree(t, prefixes))
	if r.Count() != uint64(len(prefixes)) {
		t.Fatalf("Count = %d, want %d", r.Count(), len(prefixes))
	}
	for i, want := range prefixes {
		got, err := r.Prefix(uint64(i))
		if err != nil || got != want {
			t.Errorf("Prefix(%d) = %q, %v; want %q", i, got, err, want)
		}
		name := ""
		if want != "" {
			name = strings.TrimSuffix(want, "/")
			name = name[strings.LastIndexByte(name, '/')+1:]
		}
		if got := r.ChildSegment(uint64(i)); got != name {
			t.Errorf("ChildSegment(%d) = %q, want %q", i, got, name)
		}
		if i > 0 {
			parent := want[:len(want)-len(name)-1]
			if p, n := r.ChildPrefix(parent, uint64(i)); p != want || n != name {
				t.Errorf("ChildPrefix(%q, %d) = %q, %q; want %q, %q", parent, i, p, n, want, name)
			}
		}
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = r.ChildPrefix("d0/d1/", 3) }); allocs != 1 {
		t.Errorf("ChildPrefix allocs = %v, want 1", allocs)
	}
	if _, err := r.Prefix(r.Count()); !errors.Is(err, ErrBoundsCheck) {
		t.Errorf("Prefix(out of range) err = %v, want ErrBoundsCheck", err)
	}
	if r.ChildSegment(r.Count()) != "" {
		t.Error("ChildSegment(out of range) is not empty")
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = r.Prefix(3) }); allocs != 1 {
		t.Errorf("Prefix allocs = %v, want 1", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = r.ChildSegment(3) }); allocs != 1 {
		t.Errorf("ChildSegment allocs = %v, want 1 (its copy)", allocs)
	}
}

// TestCompareChildSegments: names compare as their prefixes are
// ordered, name then slash, across two readers, without allocating.
func TestCompareChildSegments(t *testing.T) {
	a := openTree(t, writeTree(t, []string{"", "2024/", "2024-x/", "2024!/", "b/", "日本/"}))
	b := openTree(t, writeTree(t, []string{"", "2024/", "2024-x/", "b/", "c/"}))
	tests := []struct {
		pa, pb uint64
		want   int
	}{
		{1, 1, 0},  // 2024 = 2024
		{2, 1, -1}, // 2024-x < 2024: '-' < '/'
		{1, 2, 1},
		{3, 1, -1}, // 2024! < 2024: '!' < '/'
		{4, 3, 0},  // b = b
		{4, 4, -1}, // b < c
		{5, 4, 1},  // 日本 > c
		{0, 1, -1}, // root: the empty name
		{9, 1, -1}, // out of range: the empty name
	}
	for _, tt := range tests {
		if got := CompareChildSegments(a, tt.pa, b, tt.pb); got != tt.want {
			t.Errorf("CompareChildSegments(a@%d, b@%d) = %d, want %d", tt.pa, tt.pb, got, tt.want)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = CompareChildSegments(a, 2, b, 1) }); allocs != 0 {
		t.Errorf("CompareChildSegments allocs = %v, want 0", allocs)
	}
}

// TestChildSegmentOutlivesTheReader: a listing may keep a child's name
// after the index closes and its files are unmapped.
func TestChildSegmentOutlivesTheReader(t *testing.T) {
	r, err := OpenPrefixTreeReader(writeTree(t, []string{"", "alpha/", "alpha/βeta/"}))
	if err != nil {
		t.Fatal(err)
	}
	a, b := r.ChildSegment(1), r.ChildSegment(2)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if a != "alpha" || b != "βeta" {
		t.Errorf("after Close: %q, %q; want alpha, βeta", a, b)
	}
}

// TestPrefixTreeWidths: fields widen with the data, and the file is
// exactly header, descriptor, rows and tail pad.
func TestPrefixTreeWidths(t *testing.T) {
	prefixes := []string{""}
	for i := range 300 { // 300 distinct names: 2-byte IDs
		prefixes = append(prefixes, fmt.Sprintf("p%03d/", i))
	}
	prefixes = append(prefixes, "p299/child/") // parent 1 back
	for i := range 300 {                       // the last is 300 back from its parent
		prefixes = append(prefixes, fmt.Sprintf("q/%03d/", i))
	}
	prefixes = append(prefixes[:302], append([]string{"q/"}, prefixes[302:]...)...)
	dir := writeTree(t, prefixes)
	r := openTree(t, dir)
	if r.segW != 2 || r.distW != 2 {
		t.Errorf("widths = %d, %d; want 2, 2", r.segW, r.distW)
	}
	info, err := os.Stat(filepath.Join(dir, PrefixTreeFile))
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(prefixTreeDataOff + len(prefixes)*4 + prefixTreeTailPad); info.Size() != want {
		t.Errorf("file size = %d, want %d", info.Size(), want)
	}
	for i, want := range prefixes {
		if got, _ := r.Prefix(uint64(i)); got != want {
			t.Fatalf("Prefix(%d) = %q, want %q", i, got, want)
		}
	}
	if st := r.SegmentStats(); st.Count != 602 {
		t.Errorf("segments = %d, want 602 (p000..p299, child, q, 000..299)", st.Count)
	}
}

func TestPrefixTreeEmpty(t *testing.T) {
	r := openTree(t, writeTree(t, nil))
	if r.Count() != 0 {
		t.Errorf("Count = %d, want 0", r.Count())
	}
}

func TestPrefixTreeWriterRejectsBadOrder(t *testing.T) {
	w, err := NewPrefixTreeWriter(t.TempDir(), 3, 1, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := w.Add(0, 0); !errors.Is(err, errPrefixTreeOrder) {
		t.Errorf("Add before the root: err = %v", err)
	}
	if err := w.AddRoot(); err != nil {
		t.Fatal(err)
	}
	if err := w.AddRoot(); !errors.Is(err, errPrefixTreeOrder) {
		t.Errorf("second root: err = %v", err)
	}
	if err := w.Add(1, 0); !errors.Is(err, errPrefixTreeOrder) {
		t.Errorf("parent at or after the new position: err = %v", err)
	}
}

// TestPrefixTreeWriterRejectsRowsOffPlan: rows that don't match the
// count, segments and distance the widths were chosen for fail the
// writer, which leaves no file behind.
func TestPrefixTreeWriterRejectsRowsOffPlan(t *testing.T) {
	type add struct {
		parent uint64
		seg    uint32
	}
	rows := []add{{0, 0}, {0, 1}, {1, 1}} // after the root: "a/", "b/", "a/b/"
	for _, tc := range []struct {
		name     string
		count    uint64
		segments uint32
		maxDist  uint64
		rows     []add
	}{
		{"fewer rows", 5, 2, 2, rows},
		{"more rows", 3, 2, 2, rows},
		{"segment out of range", 4, 1, 2, rows},
		{"fewer segments", 4, 3, 2, rows},
		{"distance too far", 4, 2, 1, rows},
		{"distance short of planned", 4, 2, 3, rows},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewPrefixTreeWriter(dir, tc.count, tc.segments, tc.maxDist, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = w.AddRoot()
			for _, r := range tc.rows {
				if err != nil {
					break
				}
				err = w.Add(r.parent, r.seg)
			}
			if err == nil {
				err = w.Close()
			} else {
				err = errors.Join(err, w.Abort())
			}
			if !errors.Is(err, errPrefixTreePlan) {
				t.Errorf("err = %v, want errPrefixTreePlan", err)
			}
			if _, err := os.Stat(filepath.Join(dir, PrefixTreeFile)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("prefix tree left behind: %v", err)
			}
		})
	}
}

// TestPrefixTreeAnySegmentOrder: segment IDs may be given in any order
// (the builder interns in key order, the tree is in level order); every
// position still decodes to its prefix and its last segment.
func TestPrefixTreeAnySegmentOrder(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	rows := levelOrderTreeRows(r, 5, 6, 300)
	dir := t.TempDir()
	writeTreeRows(t, dir, rows, r.Perm(len(rows)))
	tree := openTree(t, dir)
	if tree.Count() != uint64(len(rows)) {
		t.Fatalf("Count = %d, want %d", tree.Count(), len(rows))
	}
	for i, row := range rows {
		if got, err := tree.Prefix(uint64(i)); err != nil || got != row.prefix {
			t.Fatalf("Prefix(%d) = %q, %v; want %q", i, got, err, row.prefix)
		}
		if got := tree.ChildSegment(uint64(i)); got != row.name {
			t.Fatalf("ChildSegment(%d) = %q, want %q", i, got, row.name)
		}
	}
}

// TestPrefixTreeCorruptDistance: a zero or overlong parent distance is
// reported, never followed.
func TestPrefixTreeCorruptDistance(t *testing.T) {
	dir := writeTree(t, []string{"", "a/", "a/b/"})
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	data, err := root.ReadFile(PrefixTreeFile)
	if err != nil {
		t.Fatal(err)
	}
	segW, rowW := int(data[HeaderSize]), int(binary.LittleEndian.Uint32(data[16:20]))
	data[prefixTreeDataOff+2*rowW+segW] = 0 // "a/b/": distance 0
	if err := root.WriteFile(PrefixTreeFile, data, FilePerm); err != nil {
		t.Fatal(err)
	}
	r := openTree(t, dir)
	if _, err := r.Prefix(2); !errors.Is(err, errPrefixTreeFile) {
		t.Errorf("Prefix with distance 0: err = %v, want errPrefixTreeFile", err)
	}
}

// TestPrefixTreeCorruptSegmentID: a row naming a segment past the
// dictionary's end fails the prefix and names no child, instead of
// reading past the dictionary.
func TestPrefixTreeCorruptSegmentID(t *testing.T) {
	dir := writeTree(t, []string{"", "a/", "a/b/"})
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	data, err := root.ReadFile(PrefixTreeFile)
	if err != nil {
		t.Fatal(err)
	}
	rowW := int(binary.LittleEndian.Uint32(data[16:20]))
	data[prefixTreeDataOff+2*rowW] = 0xff // "a/b/": segment 255 of 2
	if err := root.WriteFile(PrefixTreeFile, data, FilePerm); err != nil {
		t.Fatal(err)
	}
	r := openTree(t, dir)
	if _, err := r.Prefix(2); !errors.Is(err, errPrefixTreeFile) {
		t.Errorf("Prefix with segment 255: err = %v, want errPrefixTreeFile", err)
	}
	if got := r.ChildSegment(2); got != "" {
		t.Errorf("ChildSegment with segment 255 = %q, want empty", got)
	}
}

func TestOpenPrefixTreeReaderMissingFiles(t *testing.T) {
	for _, name := range []string{PrefixTreeFile, PrefixDictBlobFile, PrefixDictOffsetsFile} {
		dir := writeTree(t, []string{"", "a/"})
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
		if r, err := OpenPrefixTreeReader(dir); err == nil {
			r.Close()
			t.Errorf("OpenPrefixTreeReader without %s succeeded", name)
		}
	}
}

// TestInternKnownSegmentDoesNotAllocate: the builder interns one
// segment per prefix, so a known segment must cost no allocation.
func TestInternKnownSegmentDoesNotAllocate(t *testing.T) {
	si, err := NewPrefixSegmentInterner(t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer si.Close()
	if _, err := si.Intern("seg"); err != nil {
		t.Fatal(err)
	}
	buf := []byte("seg/")
	name := unsafe.String(&buf[0], 3) // defensive: Intern must accept a name over a reused buffer
	if allocs := testing.AllocsPerRun(1000, func() { _, _ = si.Intern(name) }); allocs != 0 {
		t.Errorf("Intern of a known segment allocates %v times", allocs)
	}
}
