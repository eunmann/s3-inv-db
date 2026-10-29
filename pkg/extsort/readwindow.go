package extsort

import (
	"bufio"
	"fmt"
)

// readWindow decodes records in place from a bufio.Reader's buffer: from
// the bytes one Peek returned, until a record runs past them, and only
// then discards the bytes read, in one call, and peeks again. A Peek and
// a Discard per record were a measurable part of reading a run.
type readWindow struct {
	r *bufio.Reader
	// buf is what the last Peek returned: the reader's buffer, which the
	// next Discard or read overwrites, so nothing may keep a slice of it.
	buf []byte
	off int // bytes of buf already read
}

// rest returns the bytes not yet read.
func (w *readWindow) rest() []byte {
	return w.buf[w.off:]
}

// advance marks n more bytes read.
func (w *readWindow) advance(n int) {
	w.off += n
}

// refill discards the bytes read and peeks everything buffered, reading
// the stream first if fewer than want bytes (at most the buffer's size)
// are. It returns the read's error, wrapping io.EOF at the end of the
// stream, when fewer remain.
func (w *readWindow) refill(want int) error {
	_, err := w.r.Discard(w.off)
	w.buf, w.off = nil, 0
	if err != nil {
		return fmt.Errorf("discard read bytes: %w", err)
	}
	_, err = w.r.Peek(min(want, w.r.Size()))
	// Every buffered byte, without another read: the reader cleared
	// the read's error when the first Peek returned it.
	w.buf, _ = w.r.Peek(w.r.Buffered())
	if err != nil {
		return fmt.Errorf("peek: %w", err)
	}

	return nil
}
