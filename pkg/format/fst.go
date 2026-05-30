package format

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/blevesearch/vellum"
)

// resolveAnchorEvery lets benches override DefaultAnchorEvery via
// S3INV_FST_ANCHOR_EVERY at builder construction time.
func resolveAnchorEvery() uint64 {
	if v := os.Getenv("S3INV_FST_ANCHOR_EVERY"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}

	return DefaultAnchorEvery
}

// FST-based prefix index file names.
const (
	FSTFile             = "prefixes.fst"
	FSTAnchorBlobFile   = "prefix_anchors.bin"
	FSTAnchorOffsetFile = "prefix_anchors.off.u64"
	FSTMetaFile         = "prefixes.fst.meta"
)

// DefaultAnchorEvery controls how often the FST builder stores a
// "starting key" anchor so pos→prefix reconstruction is a short
// forward walk of the FST iterator. Higher = smaller anchor file,
// slower GetPrefix. Lower = bigger anchor file, faster GetPrefix.
// Overridable via S3INV_FST_ANCHOR_EVERY for experimentation.
const DefaultAnchorEvery = 64

// FSTPrefixBuilder writes a vellum FST mapping prefix → preorder pos
// plus a sparse anchor table for pos → prefix lookup. Input prefixes
// must arrive in sorted (preorder) byte order — same constraint as
// the existing extsort merge already guarantees.
type FSTPrefixBuilder struct {
	outDir       string
	fstFile      *os.File
	vBuilder     *vellum.Builder
	anchorBlob   *BlobWriter
	anchorEvery  uint64
	count        uint64
	closed       bool
}

// NewFSTPrefixBuilder creates a builder that writes to outDir on Build.
// The fstFile is created immediately so partial writes are visible.
func NewFSTPrefixBuilder(outDir string) (*FSTPrefixBuilder, error) {
	if err := os.MkdirAll(outDir, DirPerm); err != nil {
		return nil, fmt.Errorf("create out dir: %w", err)
	}
	fstPath := filepath.Join(outDir, FSTFile)
	f, err := os.Create(fstPath)
	if err != nil {
		return nil, fmt.Errorf("create fst file: %w", err)
	}
	vb, err := vellum.New(f, nil)
	if err != nil {
		f.Close()
		os.Remove(fstPath)

		return nil, fmt.Errorf("vellum.New: %w", err)
	}
	aw, err := NewBlobWriter(
		filepath.Join(outDir, FSTAnchorBlobFile),
		filepath.Join(outDir, FSTAnchorOffsetFile),
	)
	if err != nil {
		_ = vb.Close()
		f.Close()
		os.Remove(fstPath)

		return nil, fmt.Errorf("create anchor blob writer: %w", err)
	}

	return &FSTPrefixBuilder{
		outDir:      outDir,
		fstFile:     f,
		vBuilder:    vb,
		anchorBlob:  aw,
		anchorEvery: resolveAnchorEvery(),
	}, nil
}

// Add inserts a prefix at preorder position pos. Prefixes MUST arrive
// in sorted byte order — vellum will return an error on a regression.
func (b *FSTPrefixBuilder) Add(prefix string, pos uint64) error {
	if pos != b.count {
		return fmt.Errorf("FSTPrefixBuilder: non-sequential pos %d at count %d", pos, b.count)
	}
	if err := b.vBuilder.Insert([]byte(prefix), pos); err != nil {
		return fmt.Errorf("fst insert %q: %w", prefix, err)
	}
	if b.count%b.anchorEvery == 0 {
		if err := b.anchorBlob.WriteString(prefix); err != nil {
			return fmt.Errorf("write anchor: %w", err)
		}
	}
	b.count++

	return nil
}

// Count returns the number of prefixes added so far.
func (b *FSTPrefixBuilder) Count() uint64 {
	return b.count
}

// Build finalizes the vellum FST + anchor files and writes a small
// metadata file recording the anchor stride and total prefix count.
// outDir is accepted for symmetry with StreamingMPHFBuilder but is
// already known from construction.
func (b *FSTPrefixBuilder) Build(outDir string) error {
	if b.closed {
		return errors.New("FSTPrefixBuilder.Build already called")
	}
	b.closed = true
	if err := b.vBuilder.Close(); err != nil {
		return fmt.Errorf("close vellum: %w", err)
	}
	if err := b.fstFile.Sync(); err != nil {
		return fmt.Errorf("sync fst file: %w", err)
	}
	if err := b.fstFile.Close(); err != nil {
		return fmt.Errorf("close fst file: %w", err)
	}
	if err := b.anchorBlob.Close(); err != nil {
		return fmt.Errorf("close anchor blob: %w", err)
	}

	meta := fmt.Sprintf("anchor_every=%d\ncount=%d\n", b.anchorEvery, b.count)
	if err := os.WriteFile(filepath.Join(outDir, FSTMetaFile), []byte(meta), FilePerm); err != nil {
		return fmt.Errorf("write fst meta: %w", err)
	}

	return nil
}

// Close releases resources without finalizing. Safe to call after
// Build (no-op).
func (b *FSTPrefixBuilder) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	var errs []error
	if b.vBuilder != nil {
		if err := b.vBuilder.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if b.fstFile != nil {
		_ = b.fstFile.Close()
	}
	if b.anchorBlob != nil {
		if err := b.anchorBlob.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// FSTPrefixReader reads an FST-based prefix index. Lookup uses
// vellum.Get; GetPrefix uses the anchor blob + a forward iterator
// walk capped at anchorEvery-1 steps.
type FSTPrefixReader struct {
	fst         *vellum.FST
	anchorBlob  *BlobReader
	anchorEvery uint64
	count       uint64
}

// FSTPresent reports whether the directory contains an FST-built index.
func FSTPresent(outDir string) bool {
	_, err := os.Stat(filepath.Join(outDir, FSTFile))

	return err == nil
}

// OpenFSTPrefixReader opens an FST-based prefix index from disk.
func OpenFSTPrefixReader(outDir string) (*FSTPrefixReader, error) {
	fst, err := vellum.Open(filepath.Join(outDir, FSTFile))
	if err != nil {
		return nil, fmt.Errorf("open fst: %w", err)
	}
	anchorBlob, err := OpenBlob(
		filepath.Join(outDir, FSTAnchorBlobFile),
		filepath.Join(outDir, FSTAnchorOffsetFile),
	)
	if err != nil {
		fst.Close()

		return nil, fmt.Errorf("open anchor blob: %w", err)
	}

	meta, err := readFSTMeta(outDir)
	if err != nil {
		anchorBlob.Close()
		fst.Close()

		return nil, err
	}

	return &FSTPrefixReader{
		fst:         fst,
		anchorBlob:  anchorBlob,
		anchorEvery: meta.anchorEvery,
		count:       meta.count,
	}, nil
}

type fstMeta struct {
	anchorEvery uint64
	count       uint64
}

func readFSTMeta(outDir string) (fstMeta, error) {
	data, err := os.ReadFile(filepath.Join(outDir, FSTMetaFile))
	if err != nil {
		return fstMeta{}, fmt.Errorf("read fst meta: %w", err)
	}
	var m fstMeta
	for _, line := range splitLines(string(data)) {
		k, v, ok := splitKV(line)
		if !ok {
			continue
		}
		switch k {
		case "anchor_every":
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return fstMeta{}, fmt.Errorf("parse anchor_every: %w", err)
			}
			m.anchorEvery = n
		case "count":
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return fstMeta{}, fmt.Errorf("parse count: %w", err)
			}
			m.count = n
		}
	}
	if m.anchorEvery == 0 {
		return fstMeta{}, errors.New("fst meta missing anchor_every")
	}

	return m, nil
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}

	return out
}

func splitKV(line string) (string, string, bool) {
	for i := 0; i < len(line); i++ {
		if line[i] == '=' {
			return line[:i], line[i+1:], true
		}
	}

	return "", "", false
}

// Count returns the number of prefixes in the FST.
func (r *FSTPrefixReader) Count() uint64 {
	return r.count
}

// Lookup returns the preorder position for prefix, or ok=false if
// not present.
func (r *FSTPrefixReader) Lookup(prefix string) (uint64, bool) {
	v, exists, err := r.fst.Get([]byte(prefix))
	if err != nil || !exists {
		return 0, false
	}

	return v, true
}

// GetPrefix reconstructs the prefix at preorder position pos.
// Anchor-based walk: find the nearest anchor ≤ pos, open a vellum
// iterator at the anchor's prefix, step forward (pos − anchorPos)
// times, return the current key.
func (r *FSTPrefixReader) GetPrefix(pos uint64) (string, error) {
	if pos >= r.count {
		return "", ErrBoundsCheck
	}
	anchorIdx := pos / r.anchorEvery
	steps := pos - anchorIdx*r.anchorEvery
	seed, err := r.anchorBlob.Get(anchorIdx)
	if err != nil {
		return "", fmt.Errorf("get anchor %d: %w", anchorIdx, err)
	}
	it, err := r.fst.Iterator([]byte(seed), nil)
	if err != nil {
		return "", fmt.Errorf("open iterator at anchor %d: %w", anchorIdx, err)
	}
	defer it.Close()
	for s := range steps {
		if err := it.Next(); err != nil {
			return "", fmt.Errorf("iterator next %d/%d: %w", s, steps, err)
		}
	}
	key, _ := it.Current()

	return string(key), nil
}

// GetPrefixesAscending reconstructs prefixes at the given positions in
// one pass. positions must be sorted ascending. Opens a single vellum
// iterator at the anchor for the lowest position, then walks Next()
// forward through the FST. For Browse-style queries (contiguous
// subtree ranges) this collapses N per-call iterator startups into
// one, cutting per-position cost ~25×.
func (r *FSTPrefixReader) GetPrefixesAscending(positions []uint64) ([]string, error) {
	if len(positions) == 0 {
		return nil, nil
	}
	first := positions[0]
	if first >= r.count {
		return nil, ErrBoundsCheck
	}
	anchorIdx := first / r.anchorEvery
	cur := anchorIdx * r.anchorEvery
	seed, err := r.anchorBlob.Get(anchorIdx)
	if err != nil {
		return nil, fmt.Errorf("get anchor %d: %w", anchorIdx, err)
	}
	it, err := r.fst.Iterator([]byte(seed), nil)
	if err != nil {
		return nil, fmt.Errorf("open iterator at anchor %d: %w", anchorIdx, err)
	}
	defer it.Close()

	out := make([]string, len(positions))
	for i, pos := range positions {
		if pos < cur {
			return nil, fmt.Errorf("GetPrefixesAscending: positions not sorted at index %d (pos=%d < cur=%d)", i, pos, cur)
		}
		if pos >= r.count {
			return nil, ErrBoundsCheck
		}
		for cur < pos {
			if err := it.Next(); err != nil {
				return nil, fmt.Errorf("iterator next at cur=%d toward pos=%d: %w", cur, pos, err)
			}
			cur++
		}
		key, _ := it.Current()
		out[i] = string(key)
	}

	return out, nil
}

// Close releases the FST + anchor resources.
func (r *FSTPrefixReader) Close() error {
	return errors.Join(r.fst.Close(), r.anchorBlob.Close())
}
