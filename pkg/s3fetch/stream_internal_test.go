package s3fetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	errTransient      = errors.New("connection reset by peer")
	errPrecondition   = errors.New("412 precondition failed")
	errNoSuchPart     = errors.New("no such part")
	errTestPartNumber = errors.New("GET without a part number")
)

// partServer serves an object by part number as S3 does: a part's
// bytes, its Content-Range within the object, the parts count of a
// multipart object and the ETag, honouring If-Match. Fault decides a
// GET's body; nil serves it whole.
type partServer struct {
	transfermanager.S3APIClient

	fault     func(part int32, attempt int, body []byte) io.ReadCloser
	etags     map[int32]string // a part's ETag when not etag
	data      []byte
	parts     []int // part sizes; one part when empty
	etag      string
	total     int64 // the size Content-Range reports; len(data) when 0
	attempts  map[int32]int
	ifMatch   []string
	mu        sync.Mutex
	noRange   bool // a single part without Content-Range
	noIfMatch bool // ignores If-Match, as some S3 servers do
}

func newPartServer(data []byte, parts ...int) *partServer {
	return &partServer{data: data, parts: parts, etag: `"e1"`, attempts: map[int32]int{}}
}

func (f *partServer) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if in.PartNumber == nil {
		return nil, errTestPartNumber
	}
	part := *in.PartNumber
	f.mu.Lock()
	f.attempts[part]++
	attempt := f.attempts[part]
	f.ifMatch = append(f.ifMatch, aws.ToString(in.IfMatch))
	f.mu.Unlock()

	sizes := f.parts
	if len(sizes) == 0 {
		sizes = []int{len(f.data)}
	}
	if int(part) > len(sizes) {
		return nil, errNoSuchPart
	}
	etag := f.etag
	if e, ok := f.etags[part]; ok {
		etag = e
	}
	if !f.noIfMatch && in.IfMatch != nil && *in.IfMatch != etag {
		return nil, errPrecondition
	}
	start := 0
	for _, n := range sizes[:part-1] {
		start += n
	}
	body := f.data[start : start+sizes[part-1]]
	total := f.total
	if total == 0 {
		total = int64(len(f.data))
	}
	out := &s3.GetObjectOutput{ETag: aws.String(etag), ContentLength: aws.Int64(int64(len(body)))}
	if !f.noRange && len(body) > 0 {
		out.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, start+len(body)-1, total))
	}
	if len(sizes) > 1 {
		out.PartsCount = aws.Int32(int32(len(sizes)))
	}
	out.Body = io.NopCloser(bytes.NewReader(body))
	if f.fault != nil {
		if b := f.fault(part, attempt, body); b != nil {
			out.Body = b
		}
	}

	return out, nil
}

func (f *partServer) gets(part int32) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.attempts[part]
}

// failingBody gives n bytes of body, then err.
func failingBody(body []byte, n int, err error) io.ReadCloser {
	return io.NopCloser(io.MultiReader(bytes.NewReader(body[:n]), &errReader{err: err}))
}

type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

// hangingBody blocks its first read until done closes, as a stalled
// connection does until its request is cancelled.
type hangingBody struct{ done <-chan struct{} }

func (h hangingBody) Read([]byte) (int, error) {
	<-h.done

	return 0, context.Canceled
}

func (hangingBody) Close() error { return nil }

func testData(n int) []byte {
	r := rand.New(rand.NewPCG(1, uint64(n)))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}

	return b
}

// testSizes: 1 KiB buffers, so small objects cross many buffers.
func testSizes() streamSizes {
	return streamSizes{buffer: 1024, partBuffers: 2, window: 4, attempts: 3}
}

func openTestStream(t *testing.T, srv *partServer, slots int, sizes streamSizes) (*ObjectStream, streamConfig) {
	t.Helper()
	cfg := newStreamConfig(sizes)
	s, err := openStream(t.Context(), newBoundedGets(srv, slots), cfg, "bkt", "key")
	if err != nil {
		t.Fatalf("openStream: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s, cfg
}

// readAll reads s with reads of a few odd sizes.
func readAll(s *ObjectStream) ([]byte, error) {
	var out []byte
	buf := make([]byte, 700)
	for i := 0; ; i++ {
		n, err := s.Read(buf[:1+i%len(buf)])
		out = append(out, buf[:n]...)
		if err != nil {
			return out, err
		}
	}
}

// waitGoroutines waits for the goroutine count to fall to want: a
// stream's goroutines exit just after Close sees them done.
func waitGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > want {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines, want %d: a stream goroutine outlived Close", runtime.NumGoroutine(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestObjectStream_ReadsTheObjectExactly(t *testing.T) {
	data := testData(20_000)
	for _, c := range []struct {
		srv  *partServer
		name string
	}{
		{name: "single part", srv: newPartServer(data)},
		{name: "single part, no Content-Range", srv: func() *partServer { s := newPartServer(data); s.noRange = true; return s }()},
		{name: "unequal parts", srv: newPartServer(data, 5000, 1, 3072, 4096, 7000, 831)},
		{name: "more parts than the window", srv: newPartServer(data, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000)},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := runtime.NumGoroutine()
			s, cfg := openTestStream(t, c.srv, 32, testSizes())
			if s.Size() != int64(len(data)) {
				t.Fatalf("Size = %d, want %d", s.Size(), len(data))
			}
			got, err := readAll(s)
			if !errors.Is(err, io.EOF) {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("read %d bytes that differ from the object's %d", len(got), len(data))
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if n := cfg.live.Load(); n != 0 {
				t.Errorf("%d buffers not recycled", n)
			}
			waitGoroutines(t, before)
		})
	}
}

func TestObjectStream_EmptyObject(t *testing.T) {
	srv := newPartServer(nil)
	srv.noRange = true
	s, _ := openTestStream(t, srv, 4, testSizes())
	if n, err := s.Read(make([]byte, 8)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read = %d, %v; want 0, EOF", n, err)
	}
}

// A body that fails mid-part is requested again, If-Match the ETag,
// and the reader gets exactly the object.
func TestObjectStream_RetriesAMidBodyError(t *testing.T) {
	data := testData(20_000)
	srv := newPartServer(data, 6000, 6000, 8000)
	srv.fault = func(part int32, attempt int, body []byte) io.ReadCloser {
		if attempt == 1 && part != 2 {
			return failingBody(body, 2500, errTransient)
		}

		return nil
	}
	s, _ := openTestStream(t, srv, 32, testSizes())
	got, err := readAll(s)
	if !errors.Is(err, io.EOF) || !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes (equal %v), %v; want the object and EOF", len(got), bytes.Equal(got, data), err)
	}
	if srv.gets(1) != 2 || srv.gets(2) != 1 || srv.gets(3) != 2 {
		t.Errorf("GETs per part = %d, %d, %d; want 2, 1, 2", srv.gets(1), srv.gets(2), srv.gets(3))
	}
	for i, m := range srv.ifMatch[1:] {
		if m != srv.etag {
			t.Errorf("GET %d If-Match %q, want %q", i+2, m, srv.etag)
		}
	}
}

// A body that ends short, or fails, on every attempt fails the stream
// after three: the reader sees an error, never EOF.
func TestObjectStream_PersistentBodyFaultFails(t *testing.T) {
	data := testData(20_000)
	for _, c := range []struct {
		fault func(body []byte) io.ReadCloser
		want  error
		name  string
	}{
		{name: "truncated", want: io.ErrUnexpectedEOF, fault: func(body []byte) io.ReadCloser {
			return io.NopCloser(bytes.NewReader(body[:len(body)-1]))
		}},
		{name: "failing", want: errTransient, fault: func(body []byte) io.ReadCloser {
			return failingBody(body, 100, errTransient)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := newPartServer(data, 6000, 6000, 8000)
			srv.fault = func(part int32, _ int, body []byte) io.ReadCloser {
				if part == 2 {
					return c.fault(body)
				}

				return nil
			}
			s, _ := openTestStream(t, srv, 32, testSizes())
			got, err := readAll(s)
			if errors.Is(err, io.EOF) || !errors.Is(err, c.want) {
				t.Fatalf("read error = %v, want %v", err, c.want)
			}
			if len(got) >= len(data) || !bytes.Equal(got, data[:len(got)]) {
				t.Fatalf("read %d bytes before the error; want a prefix of the object", len(got))
			}
			if n := srv.gets(2); n != 3 {
				t.Errorf("part 2 requested %d times, want 3", n)
			}
		})
	}
}

// Attempts that each read further don't run out: only attempts in a
// row that read nothing new fail the part.
func TestObjectStream_ProgressResetsAttempts(t *testing.T) {
	data := testData(10_000)
	srv := newPartServer(data)
	srv.fault = func(_ int32, attempt int, body []byte) io.ReadCloser {
		if n := attempt * 1000; n < len(body) {
			return failingBody(body, n, errTransient)
		}

		return nil
	}
	s, _ := openTestStream(t, srv, 4, testSizes())
	got, err := readAll(s)
	if !errors.Is(err, io.EOF) || !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, %v; want the object and EOF", len(got), err)
	}
	if n := srv.gets(1); n != 10 {
		t.Errorf("GETs = %d, want 10", n)
	}
}

// Parts that don't add up to the size the first response gave, or a
// part from another version of the object, fail the stream.
func TestObjectStream_InconsistentObjectFails(t *testing.T) {
	data := testData(9000)
	for _, c := range []struct {
		srv  *partServer
		want error
		name string
	}{
		{name: "parts short of the size", want: errStreamShort, srv: func() *partServer {
			s := newPartServer(data, 3000, 3000, 3000)
			s.total = 9500
			return s
		}()},
		{name: "a part's ETag differs", want: errPrecondition, srv: func() *partServer {
			s := newPartServer(data, 3000, 3000, 3000)
			s.etags = map[int32]string{3: `"e2"`}
			return s
		}()},
		{name: "a part's ETag differs, If-Match ignored", want: errObjectChanged, srv: func() *partServer {
			s := newPartServer(data, 3000, 3000, 3000)
			s.etags = map[int32]string{3: `"e2"`}
			s.noIfMatch = true
			return s
		}()},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, _ := openTestStream(t, c.srv, 32, testSizes())
			if _, err := readAll(s); !errors.Is(err, c.want) {
				t.Fatalf("read error = %v, want %v", err, c.want)
			}
		})
	}
}

// With no GET slot free the stream reads one part at a time instead of
// waiting for one.
func TestObjectStream_NoFreeSlotStillReads(t *testing.T) {
	data := testData(9000)
	srv := newPartServer(data, 3000, 3000, 3000)
	s, _ := openTestStream(t, srv, 0, testSizes())
	got, err := readAll(s)
	if !errors.Is(err, io.EOF) || !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, %v; want the object and EOF", len(got), err)
	}
}

// An unread stream holds only its first part's read-ahead; a stream
// being read holds at most window × (partBuffers + 1) + 1 buffers and
// one GET slot per part ahead of the one being read.
func TestObjectStream_ReadAheadIsBounded(t *testing.T) {
	sizes := testSizes()
	data := testData(10 * 8 * sizes.buffer)
	srv := newPartServer(data, 8*sizes.buffer, 8*sizes.buffer, 8*sizes.buffer, 8*sizes.buffer, 8*sizes.buffer,
		8*sizes.buffer, 8*sizes.buffer, 8*sizes.buffer, 8*sizes.buffer, 8*sizes.buffer)
	gets := newBoundedGets(srv, 32)
	cfg := newStreamConfig(sizes)
	s, err := openStream(t.Context(), gets, cfg, "bkt", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	settle := func(want int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for cfg.live.Load() < want && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)
		if got := cfg.live.Load(); got != want {
			t.Fatalf("%d buffers held, want %d", got, want)
		}
	}
	settle(int64(sizes.partBuffers + 1))
	if n := srv.gets(2); n != 0 {
		t.Fatalf("an unread stream requested part 2")
	}

	if _, err := s.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	settle(int64(sizes.window*(sizes.partBuffers+1) + 1))
	if n := len(gets.slots); n != sizes.window-1 {
		t.Errorf("%d GET slots held, want %d", n, sizes.window-1)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if n := len(gets.slots); n != 0 {
		t.Errorf("%d GET slots held after Close", n)
	}
	if n := cfg.live.Load(); n != 0 {
		t.Errorf("%d buffers not recycled after Close", n)
	}
}

// Cancelling the stream's context mid-read fails Read, and Close ends
// every goroutine, a hanging GET's included, and frees its slots.
func TestObjectStream_CancelLeavesNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	data := testData(9000)
	srv := newPartServer(data, 3000, 3000, 3000)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	srv.fault = func(part int32, _ int, _ []byte) io.ReadCloser {
		if part == 3 {
			return hangingBody{done: ctx.Done()}
		}

		return nil
	}
	gets := newBoundedGets(srv, 32)
	cfg := newStreamConfig(testSizes())
	s, err := openStream(ctx, gets, cfg, "bkt", "key")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(s, make([]byte, 6000)); err != nil {
		t.Fatalf("read the parts that arrive: %v", err)
	}
	cancel()
	if _, err := s.Read(make([]byte, 10)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read after cancel = %v, want context.Canceled", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if n := len(gets.slots); n != 0 {
		t.Errorf("%d GET slots held after Close", n)
	}
	if n := cfg.live.Load(); n != 0 {
		t.Errorf("%d buffers not recycled after Close", n)
	}
	waitGoroutines(t, before)
}

// Closing a stream nobody read ends its read-ahead.
func TestObjectStream_CloseUnread(t *testing.T) {
	before := runtime.NumGoroutine()
	srv := newPartServer(testData(50_000))
	cfg := newStreamConfig(testSizes())
	s, err := openStream(t.Context(), newBoundedGets(srv, 4), cfg, "bkt", "key")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := s.Read(make([]byte, 1)); !errors.Is(err, errStreamClosed) {
		t.Fatalf("Read after Close = %v, want errStreamClosed", err)
	}
	if n := cfg.live.Load(); n != 0 {
		t.Errorf("%d buffers not recycled after Close", n)
	}
	waitGoroutines(t, before)
}

func TestOpenStream_FailedFirstGET(t *testing.T) {
	_, err := openStream(t.Context(), newBoundedGets(&failGets{}, 4), newStreamConfig(testSizes()), "bkt", "key")
	if !errors.Is(err, errFakeGet) {
		t.Fatalf("openStream = %v, want errFakeGet", err)
	}
}

type failGets struct{ transfermanager.S3APIClient }

func (failGets) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, errFakeGet
}

func TestParseContentRange(t *testing.T) {
	if r, err := parseContentRange("bytes 100-199/1000"); err != nil || r != (byteRange{start: 100, length: 100, total: 1000}) {
		t.Errorf("parseContentRange = %+v, %v", r, err)
	}
	for _, bad := range []string{"", "bytes */1000", "bytes 5-4/10", "bytes 0-10/10", "bytes 0-9/*", "items 0-9/10", "bytes -1-9/10"} {
		if _, err := parseContentRange(bad); !errors.Is(err, errBadContentRange) {
			t.Errorf("parseContentRange(%q) = %v, want errBadContentRange", bad, err)
		}
	}
}

var errChecksum = errors.New("checksum did not match")

// A body that fails after its last byte, where the SDK reports a
// checksum mismatch, fails the stream without a repeat GET: its bytes
// may already be with the reader.
func TestObjectStream_FailureAfterLastByteIsFinal(t *testing.T) {
	data := testData(6000)
	srv := newPartServer(data)
	srv.fault = func(_ int32, _ int, body []byte) io.ReadCloser {
		return failingBody(body, len(body), errChecksum)
	}
	s, _ := openTestStream(t, srv, 4, testSizes())
	if _, err := readAll(s); !errors.Is(err, errChecksum) || !errors.Is(err, errPartFinal) {
		t.Fatalf("read error = %v, want errChecksum and errPartFinal", err)
	}
	if n := srv.gets(1); n != 1 {
		t.Errorf("%d GETs, want 1", n)
	}
}

// A repeat GET whose first bytes differ from those already read fails
// the stream rather than splicing two different bodies.
func TestObjectStream_RepeatMustMatchWhatWasRead(t *testing.T) {
	data := testData(6000)
	srv := newPartServer(data)
	srv.fault = func(_ int32, attempt int, body []byte) io.ReadCloser {
		if attempt > 1 {
			return nil
		}
		changed := bytes.Clone(body)
		changed[10] ^= 0xff

		return failingBody(changed, 3000, errTransient)
	}
	s, _ := openTestStream(t, srv, 4, testSizes())
	if _, err := readAll(s); !errors.Is(err, errObjectChanged) {
		t.Fatalf("read error = %v, want errObjectChanged", err)
	}
	if n := srv.gets(1); n != 2 {
		t.Errorf("%d GETs, want 2", n)
	}
}
