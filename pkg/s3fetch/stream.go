package s3fetch

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	errStreamShort     = errors.New("object stream ended before the object's size")
	errPartRange       = errors.New("part does not continue the object")
	errObjectChanged   = errors.New("object changed while it was read")
	errBadContentRange = errors.New("unreadable Content-Range")
	errStreamClosed    = errors.New("object stream closed")
)

// A part failure wrapping errPartFinal is one a repeat GET can't mend:
// its body failed after its last byte, when the SDK checks the part's
// checksum, or didn't repeat what was read.
var errPartFinal = errors.New("part failed after its bytes were read")

// streamSizes sizes an ObjectStream's read-ahead.
type streamSizes struct {
	// buffer is the bytes in one read-ahead buffer.
	buffer int
	// partBuffers is the filled buffers a part's GET queues ahead of
	// the reader; it fills one more while they wait.
	partBuffers int
	// window is the parts in flight, the one being read included, once
	// the stream is being read.
	window int
	// attempts is the GETs a part makes in a row without reading a new
	// byte before a body error fails the stream.
	attempts int
}

// Default stream sizes. Buffers are 256 KiB, so a small object takes
// little more than its size. A part in flight holds up to twelve (11
// queued, one filling), 3 MiB, and four parts are in flight, so a
// stream being read holds at most 4 × 12 + 1 buffers, 12.25 MiB, and
// one not read yet, whose only GET is its first part's, 3 MiB. Three
// attempts is the transfer manager's PartBodyMaxRetries default, which
// DownloadToReader runs with.
const (
	streamBuffer      = 256 << 10
	streamPartBuffers = 11
	streamWindow      = 4
	streamAttempts    = 3
)

func defaultStreamSizes() streamSizes {
	return streamSizes{buffer: streamBuffer, partBuffers: streamPartBuffers, window: streamWindow, attempts: streamAttempts}
}

// streamConfig is a Downloader's stream sizes and the buffers its
// streams share.
type streamConfig struct {
	streamSizes

	bufs *streamBuffers
}

func newStreamConfig(sizes streamSizes) streamConfig {
	return streamConfig{streamSizes: sizes, bufs: &streamBuffers{size: sizes.buffer}}
}

func (c streamConfig) buffer() *[]byte   { return c.bufs.get() }
func (c streamConfig) recycle(b *[]byte) { c.bufs.put(b) }

// streamBuffers recycles buffers across a Downloader's streams while
// any is open, and drops them once none is. A sync.Pool empties at
// every other GC, and a build runs many, so its streams allocated
// afresh. Opened streams overlap through a build (the next chunk opens
// before the current one closes), so the free list never holds more
// than the streams' read-ahead held at its peak.
type streamBuffers struct {
	free []*[]byte
	live atomic.Int64 // buffers out of the list
	made atomic.Int64 // buffers allocated
	size int
	open int
	mu   sync.Mutex
}

func (b *streamBuffers) get() *[]byte {
	b.live.Add(1)
	b.mu.Lock()
	if n := len(b.free); n > 0 {
		buf := b.free[n-1]
		b.free[n-1] = nil
		b.free = b.free[:n-1]
		b.mu.Unlock()

		return buf
	}
	b.mu.Unlock()
	b.made.Add(1)
	buf := make([]byte, b.size)

	return &buf
}

func (b *streamBuffers) put(buf *[]byte) {
	b.live.Add(-1)
	b.mu.Lock()
	if b.open > 0 {
		b.free = append(b.free, buf)
	}
	b.mu.Unlock()
}

func (b *streamBuffers) opened() {
	b.mu.Lock()
	b.open++
	b.mu.Unlock()
}

// closed counts a stream closed, after it put back every buffer.
func (b *streamBuffers) closed() {
	b.mu.Lock()
	b.open--
	if b.open == 0 {
		b.free = nil
	}
	b.mu.Unlock()
}

// ObjectStream reads an S3 object front to back as it downloads, with
// no temp file. It makes the GETs DownloadToReader's transfer manager
// makes, one per part by part number, so the SDK validates each part's
// checksum the same way, and each part is read through a bounded
// buffer in order. A stream is not safe for concurrent use.
//
// Read returns io.EOF only after the object's size, as its first
// response gave it, has been read: a short or failed body is an error,
// never an early end. A body that fails mid-part is requested again
// (If-Match its ETag, so a changed object fails instead) and read on
// from where it stopped, once its CRC-32C shows the repeat starts with
// the bytes already read; up to three times in a row without progress.
// Unlike DownloadToReader, a part whose checksum fails is not requested
// again: the SDK checks it at the part's end, when its bytes may
// already be with the reader, so the stream fails.
//
// Opening GETs the first part and starts reading it ahead; the other
// parts are requested once the stream is read, in order, a few ahead,
// when the Downloader's GET cap has a free slot. The part the reader
// needs next never waits for a slot: an unread stream holds its first
// GET until its reader comes, and streams that waited behind such GETs
// could wait for ever.
type ObjectStream struct {
	err        error
	cfg        streamConfig
	gets       *boundedGets
	cancel     context.CancelFunc
	launch     func(p *streamPart, out *s3.GetObjectOutput, release func())
	cur        *[]byte
	bucket     string
	key        string
	etag       string
	parts      []*streamPart // requested and not yet read through, in order
	unread     []byte
	wg         sync.WaitGroup
	size       int64
	read       int64 // bytes Read has returned
	partRead   int64 // bytes of parts[0] received
	partsCount int32
	nextPart   int32
	closed     bool
}

// streamPart is one part's GET. Its goroutine sets err, start and
// length before it sends a buffer or closes bufs.
type streamPart struct {
	err     error
	bufs    chan partBuf
	start   int64
	length  int64
	num     int32
	known   bool // start and length are set (goroutine side)
	checked bool // start has been checked (reader side)
}

type partBuf struct {
	b *[]byte
	n int
}

// OpenStream starts reading s3://bucket/key front to back for a reader
// that consumes it in order, such as a CSV chunk; Parquet needs
// DownloadToReader's random access. Size is known once it returns.
// Close ends every GET the stream started and waits for them.
func (d *Downloader) OpenStream(ctx context.Context, bucket, key string) (*ObjectStream, error) {
	return openStream(ctx, d.gets, d.streams, bucket, key)
}

func openStream(ctx context.Context, gets *boundedGets, cfg streamConfig, bucket, key string) (*ObjectStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	s := &ObjectStream{cfg: cfg, gets: gets, cancel: cancel, bucket: bucket, key: key, nextPart: 2}
	out, err := s.get(ctx, 1)
	if err != nil {
		cancel()

		return nil, fmt.Errorf("get s3://%s/%s: %w", bucket, key, err)
	}
	r, err := responseRange(out)
	if err == nil && r.start != 0 {
		err = fmt.Errorf("%w: part 1 starts at byte %d", errPartRange, r.start)
	}
	if err != nil {
		_ = out.Body.Close()
		cancel()

		return nil, fmt.Errorf("s3://%s/%s: %w", bucket, key, err)
	}
	s.size, s.etag = r.total, aws.ToString(out.ETag)
	s.partsCount = max(aws.ToInt32(out.PartsCount), 1)
	s.launch = func(p *streamPart, out *s3.GetObjectOutput, release func()) {
		s.wg.Go(func() { s.fetchPart(ctx, p, out, release) })
	}
	head := s.newPart(1)
	head.start, head.length, head.known = r.start, r.length, true
	s.parts = append(s.parts, head)
	cfg.bufs.opened()
	s.launch(head, out, nil)

	return s, nil
}

// Size is the object's size in bytes.
func (s *ObjectStream) Size() int64 { return s.size }

func (s *ObjectStream) Read(b []byte) (int, error) {
	for len(s.unread) == 0 {
		if s.err != nil {
			return 0, s.err
		}
		if err := s.advance(); err != nil {
			s.err = err

			return 0, err
		}
	}
	n := copy(b, s.unread)
	s.unread = s.unread[n:]
	s.read += int64(n)

	return n, nil
}

// advance moves to the next filled buffer, requesting parts ahead as
// it goes, and returns io.EOF once every part has been read and they
// add up to the object's size.
func (s *ObjectStream) advance() error {
	s.recycle()
	for {
		s.dispatch()
		if len(s.parts) == 0 {
			break
		}
		p := s.parts[0]
		buf, ok := <-p.bufs
		if !ok && p.err != nil {
			return p.err
		}
		if ok {
			s.cur = buf.b
		}
		if !p.checked {
			if p.start != s.read {
				return fmt.Errorf("%w: s3://%s/%s part %d starts at byte %d, after %d bytes",
					errPartRange, s.bucket, s.key, p.num, p.start, s.read)
			}
			p.checked = true
		}
		if ok {
			s.unread = (*buf.b)[:buf.n]
			s.partRead += int64(buf.n)

			return nil
		}
		if s.partRead != p.length {
			return fmt.Errorf("%w: s3://%s/%s part %d gave %d of %d bytes",
				errStreamShort, s.bucket, s.key, p.num, s.partRead, p.length)
		}
		s.parts, s.partRead = s.parts[1:], 0
	}
	if s.read != s.size {
		return fmt.Errorf("%w: s3://%s/%s gave %d of %d bytes", errStreamShort, s.bucket, s.key, s.read, s.size)
	}

	return io.EOF
}

// dispatch requests parts up to the window: the next part at once when
// none is in flight, the others only if a GET slot is free now.
func (s *ObjectStream) dispatch() {
	for s.nextPart <= s.partsCount && len(s.parts) < s.cfg.window {
		var release func()
		if len(s.parts) > 0 {
			if !s.gets.tryAcquire() {
				return
			}
			release = s.gets.release
		}
		p := s.newPart(s.nextPart)
		s.nextPart++
		s.parts = append(s.parts, p)
		s.launch(p, nil, release)
	}
}

func (s *ObjectStream) newPart(num int32) *streamPart {
	return &streamPart{num: num, bufs: make(chan partBuf, s.cfg.partBuffers)}
}

// recycle returns the buffer Read has finished with to the pool.
func (s *ObjectStream) recycle() {
	if s.cur != nil {
		s.cfg.recycle(s.cur)
		s.cur, s.unread = nil, nil
	}
}

// Close stops every GET, waits for them and recycles their buffers.
// Safe to call more than once.
func (s *ObjectStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.cancel()
	s.wg.Wait()
	for _, p := range s.parts {
		for buf := range p.bufs {
			s.cfg.recycle(buf.b)
		}
	}
	s.parts = nil
	s.recycle()
	s.cfg.bufs.closed()
	s.err = errStreamClosed

	return nil
}

// get requests one part, If-Match the object's ETag once it is known.
func (s *ObjectStream) get(ctx context.Context, part int32) (*s3.GetObjectOutput, error) {
	in := &s3.GetObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(s.key),
		PartNumber:   aws.Int32(part),
		ChecksumMode: types.ChecksumModeEnabled,
	}
	if s.etag != "" {
		in.IfMatch = aws.String(s.etag)
	}
	out, err := s.gets.S3APIClient.GetObject(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("get part %d: %w", part, err)
	}

	return out, nil
}

// getPart requests p and checks the response is p of this object: the
// same ETag and size, and on a repeat the same bytes as before.
func (s *ObjectStream) getPart(ctx context.Context, p *streamPart) (*s3.GetObjectOutput, error) {
	out, err := s.get(ctx, p.num)
	if err != nil {
		return nil, fmt.Errorf("s3://%s/%s: %w", s.bucket, s.key, err)
	}
	r, err := responseRange(out)
	switch {
	case err != nil:
	case s.etag != "" && aws.ToString(out.ETag) != s.etag:
		err = fmt.Errorf("%w: part %d has ETag %s, part 1 had %s", errObjectChanged, p.num, aws.ToString(out.ETag), s.etag)
	case r.total != s.size:
		err = fmt.Errorf("%w: part %d says %d bytes, part 1 said %d", errObjectChanged, p.num, r.total, s.size)
	case p.known && (r.start != p.start || r.length != p.length):
		err = fmt.Errorf("%w: part %d is bytes %d+%d, first %d+%d", errPartRange, p.num, r.start, r.length, p.start, p.length)
	}
	if err != nil {
		_ = out.Body.Close()

		return nil, fmt.Errorf("s3://%s/%s: %w", s.bucket, s.key, err)
	}
	if !p.known {
		p.start, p.length, p.known = r.start, r.length, true
	}

	return out, nil
}

// fetchPart reads p's body into buffers for the reader. A body that
// fails before its last byte requests the part again and skips what was
// read, checking the repeat starts with the same bytes; one that fails
// after it (the SDK's checksum) fails the stream, since its bytes may
// already be with the reader. Out is p's first response, or nil to
// request it here. Release frees p's GET slot.
func (s *ObjectStream) fetchPart(ctx context.Context, p *streamPart, out *s3.GetObjectOutput, release func()) {
	defer close(p.bufs)
	if release != nil {
		defer release()
	}
	f := partFill{cfg: s.cfg, p: p}
	defer f.drop()
	var err error
	if out == nil {
		if out, err = s.getPart(ctx, p); err != nil {
			p.err = err

			return
		}
	}
	misses := 0
	for {
		before := f.got
		err = f.copy(ctx, out.Body)
		_ = out.Body.Close()
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			p.err = fmt.Errorf("s3://%s/%s part %d: %w", s.bucket, s.key, p.num, context.Cause(ctx))

			return
		}
		if f.got > before {
			misses = 0
		}
		misses++
		if errors.Is(err, errPartFinal) || misses >= s.cfg.attempts {
			p.err = fmt.Errorf("s3://%s/%s part %d, attempt %d: %w", s.bucket, s.key, p.num, misses, err)

			return
		}
		if out, err = s.getPart(ctx, p); err != nil {
			p.err = err

			return
		}
		f.skip = f.got
	}
}

// partFill reads one part's body into pooled buffers.
type partFill struct {
	cfg  streamConfig
	p    *streamPart
	cur  *[]byte
	n    int    // bytes in cur
	got  int64  // bytes of the part read, sent or in cur
	skip int64  // bytes a repeated GET's body repeats
	crc  uint32 // of the got bytes
}

// crcTable is CRC-32C, which amd64 and arm64 compute in hardware.
//
//nolint:gochecknoglobals // a read-only table built once
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// copy reads body to its end into buffers sent to the reader. It
// checks the body is the part's whole length, and reads to EOF, where
// the SDK checks the part's checksum. An error wrapping errPartFinal
// is not worth a repeat GET.
func (f *partFill) copy(ctx context.Context, body io.Reader) error {
	if f.skip > 0 {
		repeat := crcWriter{table: crcTable}
		n, err := io.CopyN(&repeat, body, f.skip)
		f.skip -= n
		if err != nil {
			return fmt.Errorf("skip the %d bytes already read: %w", f.got, err)
		}
		if repeat.crc != f.crc {
			return fmt.Errorf("%w: %w: part %d's first %d bytes differ from those read", errPartFinal, errObjectChanged, f.p.num, f.got)
		}
	}
	for {
		if f.cur != nil && f.n == len(*f.cur) {
			if err := f.send(ctx); err != nil {
				return err
			}
		}
		if f.cur == nil {
			f.cur, f.n = f.cfg.buffer(), 0
		}
		n, err := body.Read((*f.cur)[f.n:])
		f.crc = crc32.Update(f.crc, crcTable, (*f.cur)[f.n:f.n+n])
		f.n += n
		f.got += int64(n)
		if f.got > f.p.length {
			return fmt.Errorf("%w: %w: part %d body is over %d bytes", errPartFinal, errPartRange, f.p.num, f.p.length)
		}
		if errors.Is(err, io.EOF) {
			if f.got < f.p.length {
				return fmt.Errorf("part %d body ended at byte %d of %d: %w", f.p.num, f.got, f.p.length, io.ErrUnexpectedEOF)
			}

			return f.send(ctx)
		}
		if err != nil {
			if f.got == f.p.length {
				return fmt.Errorf("%w: part %d: %w", errPartFinal, f.p.num, err)
			}

			return fmt.Errorf("read part %d: %w", f.p.num, err)
		}
	}
}

// crcWriter checksums what is written to it.
type crcWriter struct {
	table *crc32.Table
	crc   uint32
}

func (w *crcWriter) Write(p []byte) (int, error) {
	w.crc = crc32.Update(w.crc, w.table, p)

	return len(p), nil
}

// send hands the filled part of cur to the reader.
func (f *partFill) send(ctx context.Context) error {
	if f.n == 0 {
		return nil
	}
	select {
	case f.p.bufs <- partBuf{b: f.cur, n: f.n}:
		f.cur, f.n = nil, 0

		return nil
	case <-ctx.Done():
		return fmt.Errorf("send part %d: %w", f.p.num, context.Cause(ctx))
	}
}

func (f *partFill) drop() {
	if f.cur != nil {
		f.cfg.recycle(f.cur)
		f.cur = nil
	}
}

// byteRange is the bytes a response holds and the object's size.
type byteRange struct {
	start, length, total int64
}

// responseRange reads a part GET's Content-Range, or takes the whole
// object from Content-Length without one, and checks the two agree.
func responseRange(out *s3.GetObjectOutput) (byteRange, error) {
	if out.ContentRange == nil {
		if out.ContentLength == nil {
			return byteRange{}, fmt.Errorf("%w: no Content-Range or Content-Length", errBadContentRange)
		}
		n := *out.ContentLength

		return byteRange{start: 0, length: n, total: n}, nil
	}
	r, err := parseContentRange(*out.ContentRange)
	if err != nil {
		return byteRange{}, err
	}
	if out.ContentLength != nil && *out.ContentLength != r.length {
		return byteRange{}, fmt.Errorf("%w: %q with Content-Length %d", errBadContentRange, *out.ContentRange, *out.ContentLength)
	}

	return r, nil
}

// parseContentRange parses "bytes first-last/total".
func parseContentRange(v string) (byteRange, error) {
	spec, ok := strings.CutPrefix(v, "bytes ")
	span, totalText, ok2 := strings.Cut(spec, "/")
	firstText, lastText, ok3 := strings.Cut(span, "-")
	if !ok || !ok2 || !ok3 {
		return byteRange{}, fmt.Errorf("%w: %q", errBadContentRange, v)
	}
	first, err1 := strconv.ParseInt(firstText, 10, 64)
	last, err2 := strconv.ParseInt(lastText, 10, 64)
	total, err3 := strconv.ParseInt(totalText, 10, 64)
	if err := errors.Join(err1, err2, err3); err != nil || first < 0 || last < first || last >= total {
		return byteRange{}, fmt.Errorf("%w: %q", errBadContentRange, v)
	}

	return byteRange{start: first, length: last - first + 1, total: total}, nil
}
