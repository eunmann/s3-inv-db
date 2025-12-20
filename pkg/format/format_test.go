package format

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   12345,
		Width:   8,
	}

	encoded := EncodeHeader(h)
	if len(encoded) != HeaderSize {
		t.Fatalf("encoded size = %d, want %d", len(encoded), HeaderSize)
	}

	decoded, err := DecodeHeader(encoded)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}

	if decoded.Magic != h.Magic {
		t.Errorf("Magic = %x, want %x", decoded.Magic, h.Magic)
	}
	if decoded.Version != h.Version {
		t.Errorf("Version = %d, want %d", decoded.Version, h.Version)
	}
	if decoded.Count != h.Count {
		t.Errorf("Count = %d, want %d", decoded.Count, h.Count)
	}
	if decoded.Width != h.Width {
		t.Errorf("Width = %d, want %d", decoded.Width, h.Width)
	}
}

func TestDecodeHeaderTooShort(t *testing.T) {
	_, err := DecodeHeader(make([]byte, HeaderSize-1))
	if !errors.Is(err, ErrInvalidHeader) {
		t.Errorf("DecodeHeader(short) = %v, want ErrInvalidHeader", err)
	}
}

func TestArrayWriterU64(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.u64")

	w, err := NewArrayWriter(path, 8)
	if err != nil {
		t.Fatalf("NewArrayWriter failed: %v", err)
	}

	values := []uint64{100, 200, 300, 400, 500}
	for _, v := range values {
		if err := w.WriteU64(v); err != nil {
			t.Fatalf("WriteU64 failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read back
	r, err := OpenArray(path)
	if err != nil {
		t.Fatalf("OpenArray failed: %v", err)
	}
	defer r.Close()

	if r.Count() != uint64(len(values)) {
		t.Errorf("Count = %d, want %d", r.Count(), len(values))
	}

	for i, expected := range values {
		val, err := r.GetU64(uint64(i))
		if err != nil {
			t.Errorf("GetU64(%d) failed: %v", i, err)
		}
		if val != expected {
			t.Errorf("GetU64(%d) = %d, want %d", i, val, expected)
		}
	}
}

func TestArrayWriterU32(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.u32")

	w, err := NewArrayWriter(path, 4)
	if err != nil {
		t.Fatalf("NewArrayWriter failed: %v", err)
	}

	values := []uint32{10, 20, 30}
	for _, v := range values {
		if err := w.WriteU32(v); err != nil {
			t.Fatalf("WriteU32 failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := OpenArray(path)
	if err != nil {
		t.Fatalf("OpenArray failed: %v", err)
	}
	defer r.Close()

	for i, expected := range values {
		val, err := r.GetU32(uint64(i))
		if err != nil {
			t.Errorf("GetU32(%d) failed: %v", i, err)
		}
		if val != expected {
			t.Errorf("GetU32(%d) = %d, want %d", i, val, expected)
		}
	}
}

func TestArrayWriterU16(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.u16")

	w, err := NewArrayWriter(path, 2)
	if err != nil {
		t.Fatalf("NewArrayWriter failed: %v", err)
	}

	values := []uint16{1, 2, 3, 65535}
	for _, v := range values {
		if err := w.WriteU16(v); err != nil {
			t.Fatalf("WriteU16 failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := OpenArray(path)
	if err != nil {
		t.Fatalf("OpenArray failed: %v", err)
	}
	defer r.Close()

	for i, expected := range values {
		val, err := r.GetU16(uint64(i))
		if err != nil {
			t.Errorf("GetU16(%d) failed: %v", i, err)
		}
		if val != expected {
			t.Errorf("GetU16(%d) = %d, want %d", i, val, expected)
		}
	}
}

func TestArrayReaderBoundsCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.u64")

	w, err := NewArrayWriter(path, 8)
	if err != nil {
		t.Fatalf("NewArrayWriter failed: %v", err)
	}
	_ = w.WriteU64(100)
	_ = w.WriteU64(200)
	_ = w.Close()

	r, err := OpenArray(path)
	if err != nil {
		t.Fatalf("OpenArray failed: %v", err)
	}
	defer r.Close()

	_, err = r.GetU64(2)
	if !errors.Is(err, ErrBoundsCheck) {
		t.Errorf("GetU64(out of bounds) = %v, want ErrBoundsCheck", err)
	}
}

func TestArrayWidthMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.u64")

	w, err := NewArrayWriter(path, 8)
	if err != nil {
		t.Fatalf("NewArrayWriter failed: %v", err)
	}

	err = w.WriteU32(100)
	if err == nil {
		t.Error("expected width mismatch error")
	}
	_ = w.Close()
}

func TestBlobWriterReader(t *testing.T) {
	dir := t.TempDir()
	blobPath := filepath.Join(dir, "prefix_blob.bin")
	offsetsPath := filepath.Join(dir, "prefix_offsets.u64")

	w, err := NewBlobWriter(blobPath, offsetsPath)
	if err != nil {
		t.Fatalf("NewBlobWriter failed: %v", err)
	}

	strings := []string{"", "a/", "a/b/", "b/", "日本語/"}
	for _, s := range strings {
		if err := w.WriteString(s); err != nil {
			t.Fatalf("WriteString failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := OpenBlob(blobPath, offsetsPath)
	if err != nil {
		t.Fatalf("OpenBlob failed: %v", err)
	}
	defer r.Close()

	if r.Count() != uint64(len(strings)) {
		t.Errorf("Count = %d, want %d", r.Count(), len(strings))
	}

	for i, expected := range strings {
		s, err := r.Get(uint64(i))
		if err != nil {
			t.Errorf("Get(%d) failed: %v", i, err)
		}
		if s != expected {
			t.Errorf("Get(%d) = %q, want %q", i, s, expected)
		}
	}
}

func TestBlobReaderUnsafeGet(t *testing.T) {
	dir := t.TempDir()
	blobPath := filepath.Join(dir, "prefix_blob.bin")
	offsetsPath := filepath.Join(dir, "prefix_offsets.u64")

	w, err := NewBlobWriter(blobPath, offsetsPath)
	if err != nil {
		t.Fatalf("NewBlobWriter failed: %v", err)
	}

	_ = w.WriteString("hello")
	_ = w.WriteString("world")
	_ = w.Close()

	r, err := OpenBlob(blobPath, offsetsPath)
	if err != nil {
		t.Fatalf("OpenBlob failed: %v", err)
	}
	defer r.Close()

	if r.UnsafeGet(0) != "hello" {
		t.Errorf("UnsafeGet(0) = %q, want hello", r.UnsafeGet(0))
	}
	if r.UnsafeGet(1) != "world" {
		t.Errorf("UnsafeGet(1) = %q, want world", r.UnsafeGet(1))
	}
}

func TestBlobReaderBoundsCheck(t *testing.T) {
	dir := t.TempDir()
	blobPath := filepath.Join(dir, "prefix_blob.bin")
	offsetsPath := filepath.Join(dir, "prefix_offsets.u64")

	w, err := NewBlobWriter(blobPath, offsetsPath)
	if err != nil {
		t.Fatalf("NewBlobWriter failed: %v", err)
	}
	_ = w.WriteString("test")
	_ = w.Close()

	r, err := OpenBlob(blobPath, offsetsPath)
	if err != nil {
		t.Fatalf("OpenBlob failed: %v", err)
	}
	defer r.Close()

	_, err = r.Get(1)
	if !errors.Is(err, ErrBoundsCheck) {
		t.Errorf("Get(out of bounds) = %v, want ErrBoundsCheck", err)
	}
}

func TestOpenArrayBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.bin")

	// Write file with wrong magic
	header := EncodeHeader(Header{
		Magic:   0xDEADBEEF,
		Version: Version,
		Count:   0,
		Width:   8,
	})
	if err := os.WriteFile(path, header, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := OpenArray(path)
	if !errors.Is(err, ErrMagicMismatch) {
		t.Errorf("OpenArray(bad magic) = %v, want ErrMagicMismatch", err)
	}
}

func TestOpenArrayBadVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.bin")

	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: 999,
		Count:   0,
		Width:   8,
	})
	if err := os.WriteFile(path, header, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := OpenArray(path)
	if !errors.Is(err, ErrVersionMismatch) {
		t.Errorf("OpenArray(bad version) = %v, want ErrVersionMismatch", err)
	}
}

func TestOpenArrayFileTooSmall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "small.bin")

	// Write header claiming 10 elements but file is too small
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   10,
		Width:   8,
	})
	if err := os.WriteFile(path, header, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := OpenArray(path)
	if err == nil {
		t.Error("expected error for file too small")
	}
}

func TestMmapEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.bin")

	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	m, err := OpenMmap(path)
	if err != nil {
		t.Fatalf("OpenMmap failed: %v", err)
	}
	defer m.Close()

	if m.Size() != 0 {
		t.Errorf("Size = %d, want 0", m.Size())
	}
	if m.Data() != nil {
		t.Errorf("Data should be nil for empty file")
	}
}

func TestArrayUnsafeGet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.u64")

	w, err := NewArrayWriter(path, 8)
	if err != nil {
		t.Fatalf("NewArrayWriter failed: %v", err)
	}
	_ = w.WriteU64(111)
	_ = w.WriteU64(222)
	_ = w.WriteU64(333)
	_ = w.Close()

	r, err := OpenArray(path)
	if err != nil {
		t.Fatalf("OpenArray failed: %v", err)
	}
	defer r.Close()

	if r.UnsafeGetU64(0) != 111 {
		t.Errorf("UnsafeGetU64(0) = %d, want 111", r.UnsafeGetU64(0))
	}
	if r.UnsafeGetU64(1) != 222 {
		t.Errorf("UnsafeGetU64(1) = %d, want 222", r.UnsafeGetU64(1))
	}
	if r.UnsafeGetU64(2) != 333 {
		t.Errorf("UnsafeGetU64(2) = %d, want 333", r.UnsafeGetU64(2))
	}
}

func TestLargeArray(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.u64")

	n := 10000
	w, err := NewArrayWriter(path, 8)
	if err != nil {
		t.Fatalf("NewArrayWriter failed: %v", err)
	}

	for i := range n {
		if err := w.WriteU64(uint64(i * 100)); err != nil {
			t.Fatalf("WriteU64 failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	r, err := OpenArray(path)
	if err != nil {
		t.Fatalf("OpenArray failed: %v", err)
	}
	defer r.Close()

	if r.Count() != uint64(n) {
		t.Errorf("Count = %d, want %d", r.Count(), n)
	}

	// Spot check
	for _, i := range []int{0, 100, 5000, 9999} {
		val, err := r.GetU64(uint64(i))
		if err != nil {
			t.Errorf("GetU64(%d) failed: %v", i, err)
		}
		expected := uint64(i * 100)
		if val != expected {
			t.Errorf("GetU64(%d) = %d, want %d", i, val, expected)
		}
	}
}
