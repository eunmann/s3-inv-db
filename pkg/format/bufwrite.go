package format

import (
	"bufio"
	"encoding/binary"
)

// putU64 encodes into w's own free space (AvailableBuffer) instead of a
// local array: a local array passed to Write escapes to the heap, one
// allocation per value on paths that write one value per prefix or row.
func putU64(w *bufio.Writer, v uint64) error {
	_, err := w.Write(binary.LittleEndian.AppendUint64(w.AvailableBuffer(), v))

	return err //nolint:wrapcheck // callers wrap with their own context
}
