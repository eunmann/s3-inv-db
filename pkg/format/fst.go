package format

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blevesearch/vellum"
)

// FSTFile is the on-disk name for the vellum FST that maps each
// prefix string to its preorder position. Present iff the index was
// built with FST-mode forward lookup (Option A). The prefix-dict /
// prefix-blob files remain on disk and handle pos→prefix reads.
const FSTFile = "prefixes.fst"

// Sentinel errors for err113 / linter.
var (
	errFSTNonSequentialPos = errors.New("FSTPrefixBuilder: non-sequential pos")
	errFSTAlreadyBuilt     = errors.New("FSTPrefixBuilder.Build already called")
)

// FSTPrefixBuilder writes a vellum FST mapping prefix → preorder
// position. Input prefixes must arrive in sorted byte order — the
// extsort merge stage already enforces this for the project's
// preorder-major key shape. Used as a sidecar to the existing
// prefix-blob/prefix-dict writers when FST mode is enabled on the
// IndexBuilder: forward Lookup goes through the FST at read time,
// reverse GetPrefix(pos) keeps going through prefix-blob/dict.
type FSTPrefixBuilder struct {
	path  string
	file  *os.File
	vb    *vellum.Builder
	count uint64
}

// NewFSTPrefixBuilder creates a builder that streams a vellum FST
// to outDir/prefixes.fst.
func NewFSTPrefixBuilder(outDir string) (*FSTPrefixBuilder, error) {
	if err := os.MkdirAll(outDir, DirPerm); err != nil {
		return nil, fmt.Errorf("create out dir: %w", err)
	}
	path := filepath.Join(outDir, FSTFile)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create fst file: %w", err)
	}
	vb, err := vellum.New(f, nil)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)

		return nil, fmt.Errorf("vellum.New: %w", err)
	}

	return &FSTPrefixBuilder{path: path, file: f, vb: vb}, nil
}

// Add inserts prefix → pos. Pos is expected to equal the running
// Count() so the FST keys arrive in preorder; vellum will error if
// the byte order goes backwards.
func (b *FSTPrefixBuilder) Add(prefix string, pos uint64) error {
	if pos != b.count {
		return fmt.Errorf("%w: pos=%d count=%d", errFSTNonSequentialPos, pos, b.count)
	}
	if err := b.vb.Insert([]byte(prefix), pos); err != nil {
		return fmt.Errorf("fst insert %q: %w", prefix, err)
	}
	b.count++

	return nil
}

// Count returns the number of prefixes inserted.
func (b *FSTPrefixBuilder) Count() uint64 { return b.count }

// Build finalizes the FST. OutDir is accepted for symmetry with the
// other builders but is implied by NewFSTPrefixBuilder.
func (b *FSTPrefixBuilder) Build(_ string) error {
	if b.vb == nil {
		return errFSTAlreadyBuilt
	}
	if err := b.vb.Close(); err != nil {
		_ = b.file.Close()

		return fmt.Errorf("close vellum: %w", err)
	}
	b.vb = nil
	if err := b.file.Sync(); err != nil {
		_ = b.file.Close()

		return fmt.Errorf("sync fst file: %w", err)
	}
	if err := b.file.Close(); err != nil {
		return fmt.Errorf("close fst file: %w", err)
	}
	b.file = nil

	return nil
}

// Close releases resources without finalising. Safe after Build.
func (b *FSTPrefixBuilder) Close() error {
	var errs []error
	if b.vb != nil {
		if err := b.vb.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close vellum: %w", err))
		}
		b.vb = nil
	}
	if b.file != nil {
		if err := b.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close fst file: %w", err))
		}
		b.file = nil
	}

	return errors.Join(errs...)
}

// FSTPrefixReader reads the vellum FST. Forward-only — pos→prefix
// reverse lookup is intentionally NOT served here; callers use
// prefix-dict / prefix-blob for that (Option A keeps both files).
type FSTPrefixReader struct {
	fst   *vellum.FST
	count uint64
}

// FSTPresent reports whether the directory has a prefixes.fst file.
func FSTPresent(outDir string) bool {
	_, err := os.Stat(filepath.Join(outDir, FSTFile))

	return err == nil
}

// OpenFSTPrefixReader opens the FST from outDir/prefixes.fst.
func OpenFSTPrefixReader(outDir string) (*FSTPrefixReader, error) {
	fst, err := vellum.Open(filepath.Join(outDir, FSTFile))
	if err != nil {
		return nil, fmt.Errorf("open fst: %w", err)
	}

	return &FSTPrefixReader{fst: fst, count: uint64(fst.Len())}, nil
}

// Count returns the number of prefixes in the FST.
func (r *FSTPrefixReader) Count() uint64 { return r.count }

// Lookup returns the preorder position for prefix, or ok=false if
// absent. Vellum.Get is exact — no fingerprint-collision risk.
func (r *FSTPrefixReader) Lookup(prefix string) (uint64, bool) {
	v, exists, err := r.fst.Get([]byte(prefix))
	if err != nil || !exists {
		return 0, false
	}

	return v, true
}

// Close releases the FST mmap.
func (r *FSTPrefixReader) Close() error {
	if r == nil || r.fst == nil {
		return nil
	}
	err := r.fst.Close()
	r.fst = nil
	if err != nil {
		return fmt.Errorf("close fst: %w", err)
	}

	return nil
}
