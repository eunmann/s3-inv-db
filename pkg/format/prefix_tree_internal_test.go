package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unsafe"
)

// writeTree writes a prefix tree for prefixes (in preorder, root first,
// each after its parent) into a fresh directory.
func writeTree(t *testing.T, prefixes []string) string {
	t.Helper()
	dir := t.TempDir()
	w, err := NewPrefixTreeWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	type open struct {
		prefix string
		pos    uint64
	}
	var stack []open
	for i, p := range prefixes {
		for len(stack) > 0 && !strings.HasPrefix(p, stack[len(stack)-1].prefix) {
			stack = stack[:len(stack)-1]
		}
		if len(stack) == 0 {
			if err := w.AddRoot(); err != nil {
				t.Fatal(err)
			}
		} else {
			parent := stack[len(stack)-1]
			name := strings.TrimSuffix(strings.TrimPrefix(p, parent.prefix), "/")
			if err := w.Add(parent.pos, name); err != nil {
				t.Fatalf("Add(%q): %v", p, err)
			}
		}
		stack = append(stack, open{p, uint64(i)})
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	return dir
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
	if allocs := testing.AllocsPerRun(100, func() { _ = r.ChildSegment(3) }); allocs != 0 {
		t.Errorf("ChildSegment allocs = %v, want 0", allocs)
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
	w, err := NewPrefixTreeWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := w.Add(0, "a"); !errors.Is(err, errPrefixTreeOrder) {
		t.Errorf("Add before the root: err = %v", err)
	}
	if err := w.AddRoot(); err != nil {
		t.Fatal(err)
	}
	if err := w.AddRoot(); !errors.Is(err, errPrefixTreeOrder) {
		t.Errorf("second root: err = %v", err)
	}
	if err := w.Add(1, "a"); !errors.Is(err, errPrefixTreeOrder) {
		t.Errorf("parent at or after the new position: err = %v", err)
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

// TestPrefixTreeAddDoesNotAllocate: the builder adds one row per prefix,
// so a known segment must cost no allocation.
func TestPrefixTreeAddDoesNotAllocate(t *testing.T) {
	w, err := NewPrefixTreeWriter(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := w.AddRoot(); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(0, "seg"); err != nil {
		t.Fatal(err)
	}
	buf := []byte("seg/")
	name := unsafe.String(&buf[0], 3) // defensive: Add must accept a name over a reused buffer
	if allocs := testing.AllocsPerRun(1000, func() { _ = w.Add(0, name) }); allocs != 0 {
		t.Errorf("Add of a known segment allocates %v times", allocs)
	}
}

func TestPrefixTreeMatches(t *testing.T) {
	prefixes := []string{"", "/", "a/", "a//", "a/b/", "ab/", "日本語/", "日本語/x/"}
	r := openTree(t, writeTree(t, prefixes))
	for pos, p := range prefixes {
		for _, q := range append(slices.Clone(prefixes), "a", "a/b", "b/", "a/b/c/", "x/a/b/", "//") {
			if got, want := r.Matches(uint64(pos), q), q == p; got != want {
				t.Errorf("Matches(%d %q, %q) = %v, want %v", pos, p, q, got, want)
			}
		}
	}
	if r.Matches(r.Count(), "") {
		t.Error("Matches(out of range) = true")
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = r.Matches(7, "日本語/x/") }); allocs != 0 {
		t.Errorf("Matches allocates %v times", allocs)
	}
}
