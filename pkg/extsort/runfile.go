package extsort

import (
	"errors"
	"fmt"
	"os"

	"github.com/rs/zerolog"
)

// DefaultRunBufferSize is the default I/O buffer for run-file reader
// and writer instances when the caller passes 0. Big enough to
// amortise syscall overhead on sequential append/scan, small enough
// that holding several per worker stays well under any memory cap.
//
// Exported so benches and integration tests can reuse the same default
// without duplicating the literal across files.
const DefaultRunBufferSize = 4 * 1024 * 1024

// Sentinel errors for the extsort package. Wrap with %w when adding
// context via fmt.Errorf so callers can match with errors.Is.
var (
	// ErrInvalidMagic indicates a run file has the wrong magic number.
	ErrInvalidMagic = errors.New("invalid magic")
	// ErrUnsupportedVersion indicates an unsupported run file format version.
	ErrUnsupportedVersion = errors.New("unsupported run file version")
	// ErrNotCompressed indicates the file is not compressed when a
	// compressed reader is required.
	ErrNotCompressed = errors.New("file is not compressed")
	// ErrUnsupportedCompression indicates an unsupported compression type.
	ErrUnsupportedCompression = errors.New("unsupported compression type")
	// ErrNoInputPaths indicates a merge call received no input paths.
	ErrNoInputPaths = errors.New("no input paths provided")
)

// removeFiles best-effort deletes each path. Used on cancellation /
// failure paths in the merge pipeline where the original error is the
// one that matters and a missed cleanup is bounded to temp files.
func removeFiles(paths []string) {
	for _, p := range paths {
		_ = os.Remove(p)
	}
}

// logCleanupErr logs a cleanup failure at Warn so a missed temp-file
// delete is visible without masking the originating error.
func logCleanupErr(log *zerolog.Logger, op, path string, err error) {
	if err == nil || os.IsNotExist(err) {
		return
	}
	log.Warn().Str("op", op).Str("path", path).Err(err).Msg("cleanup failed")
}

// writeAndCloseRun writes rows, already sorted by prefix, through w; on
// a write error it closes w and deletes runPath before returning. On
// success it returns the close result so the caller can react to a
// close-only failure.
func writeAndCloseRun(w *CompressedRunWriter, rows []*PrefixRow, runPath string) error {
	if err := w.WriteAll(rows); err != nil {
		_ = w.Close()
		_ = os.Remove(runPath)

		return fmt.Errorf("write run: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close run writer: %w", err)
	}

	return nil
}
