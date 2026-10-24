package format

import (
	"bufio"
	"encoding/binary"
)

// The put* helpers encode into w's own free space (AvailableBuffer)
// instead of a local array: a local array passed to Write escapes to
// the heap, one allocation per value on paths that write one value per
// prefix or row.

func putU16(w *bufio.Writer, v uint16) error {
	_, err := w.Write(binary.LittleEndian.AppendUint16(w.AvailableBuffer(), v))

	return err //nolint:wrapcheck // callers wrap with their own context
}

func putU32(w *bufio.Writer, v uint32) error {
	_, err := w.Write(binary.LittleEndian.AppendUint32(w.AvailableBuffer(), v))

	return err //nolint:wrapcheck // callers wrap with their own context
}

func putU64(w *bufio.Writer, v uint64) error {
	_, err := w.Write(binary.LittleEndian.AppendUint64(w.AvailableBuffer(), v))

	return err //nolint:wrapcheck // callers wrap with their own context
}
