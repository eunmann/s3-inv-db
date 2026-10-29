package format

import (
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestCRC32CCombineMatchesConcatenation(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)
	r := rand.New(rand.NewPCG(1, 2))
	data := randomBytes(r, 3*testPiece+17)
	end := len(data)
	const random = 1000
	cuts := make([][3]int, 0, random+6)
	cuts = append(cuts, [3]int{0, 0, 0}, [3]int{0, 0, end}, [3]int{0, end, end}, [3]int{end, end, end},
		[3]int{5, 5, 5}, [3]int{1, 2, 3})
	for range random {
		c := [3]int{r.IntN(end + 1), r.IntN(end + 1), r.IntN(end + 1)}
		slices.Sort(c[:])
		cuts = append(cuts, c)
	}
	for _, c := range cuts {
		lo, mid, hi := c[0], c[1], c[2]
		left, right := data[lo:mid], data[mid:hi]
		got := crc32cCombine(crc32.Checksum(left, table), crc32.Checksum(right, table), int64(len(right)))
		if want := crc32.Checksum(data[lo:hi], table); got != want {
			t.Fatalf("combine of [%d, %d) and [%d, %d) = %08x, want %08x", lo, mid, mid, hi, got, want)
		}
	}
	for _, n := range []int{0, 1, 7, 8, 63, 64, 1000, testPiece} {
		if got, want := crc32cZeros(int64(n)), crc32.Checksum(make([]byte, n), table); got != want {
			t.Errorf("crc32cZeros(%d) = %08x, want %08x", n, got, want)
		}
	}
}

// writeOp is one write a test makes through a hashedFile.
type writeOp struct {
	data     []byte
	off      int64 // for at
	at       bool  // WriteAt, else Write at the sequential offset
	seek     int64 // seek to this first, when set
	truncate int64 // truncate to this first, when set
}

// hashedChecksum writes ops through a hashedFile at path and returns
// the checksum it recorded, if any.
func hashedChecksum(t *testing.T, path string, ops []writeOp) (string, bool) {
	t.Helper()
	sums := newChecksums(testPiece)
	f, err := createHashedFile(path, sums)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range ops {
		if op.truncate > 0 {
			if err := f.truncate(op.truncate); err != nil {
				t.Fatal(err)
			}
		}
		if op.seek > 0 {
			if err := f.seek(op.seek); err != nil {
				t.Fatal(err)
			}
		}
		if op.at {
			_, err = f.WriteAt(op.data, op.off)
		} else {
			_, err = f.Write(op.data)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	return sums.lookup(path, info)
}

func randomBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}

	return b
}

// TestHashedFileChecksumMatchesReread: whatever order and offsets a
// file's bytes are written in, sequentially, at offsets, across pieces,
// after a skipped header or into a truncated file, the checksum its
// writes recorded is the one reading it back gives.
func TestHashedFileChecksumMatchesReread(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	sequential := func(sizes ...int) []writeOp {
		ops := make([]writeOp, 0, len(sizes))
		for _, n := range sizes {
			ops = append(ops, writeOp{data: randomBytes(r, n)})
		}

		return ops
	}
	shuffledAt := func(total, parts int) []writeOp {
		ops := make([]writeOp, 0, parts)
		cuts := make([]int, 1, parts+1)
		for range parts - 1 {
			cuts = append(cuts, r.IntN(total+1))
		}
		cuts = append(cuts, total)
		slices.Sort(cuts)
		for i := range parts {
			ops = append(ops, writeOp{data: randomBytes(r, cuts[i+1]-cuts[i]), off: int64(cuts[i]), at: true})
		}
		r.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })

		return ops
	}
	cases := map[string][]writeOp{
		"empty":               nil,
		"one byte":            sequential(1),
		"exact piece":         sequential(testPiece),
		"many small":          sequential(1, 2, 3, 100, 5000, 7, testPiece+3, 9000, 1),
		"zero-length writes":  sequential(0, 10, 0, testPiece, 0),
		"at offsets shuffled": shuffledAt(5*testPiece+123, 40),
		"skipped header": append(append([]writeOp{{seek: HeaderSize}}, sequential(3*testPiece)...),
			writeOp{data: randomBytes(r, HeaderSize), at: true}),
		"truncated then filled": append([]writeOp{{truncate: 4*testPiece + 8}},
			writeOp{data: randomBytes(r, 2*testPiece), off: testPiece, at: true},
			writeOp{data: randomBytes(r, 40), at: true}),
		"hole between writes": {
			{data: randomBytes(r, 100)},
			{data: randomBytes(r, 50), off: 3*testPiece - 10, at: true},
		},
		"sequential around writes at offsets": {
			{data: randomBytes(r, 100)},
			{data: randomBytes(r, 300), off: 5000, at: true},
			{data: randomBytes(r, 200)},
			{seek: 5300, data: randomBytes(r, 2*testPiece)},
			{data: randomBytes(r, 4700), off: 300, at: true},
		},
		"header then sequential": {
			{data: randomBytes(r, HeaderSize), at: true},
			{seek: HeaderSize, data: randomBytes(r, testPiece+5)},
		},
	}
	for name, ops := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			got, ok := hashedChecksum(t, filepath.Join(dir, "f.bin"), ops)
			if !ok {
				t.Fatal("no checksum recorded")
			}
			if want := checksumOf(t, dir, "f.bin"); got != want {
				t.Errorf("recorded checksum %s, reread %s", got, want)
			}
		})
	}
}

// TestHashedFileNoChecksumWhenUnsure: bytes written twice or cut off by
// a truncate leave no checksum, so the manifest reads the file.
func TestHashedFileNoChecksumWhenUnsure(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	for name, ops := range map[string][]writeOp{
		"overwritten header": {{data: randomBytes(r, 3*testPiece)}, {data: randomBytes(r, HeaderSize), at: true}},
		"overlapping at":     {{data: randomBytes(r, 100), off: 50, at: true}, {data: randomBytes(r, 100), at: true}},
		"shrunk":             {{data: randomBytes(r, 1000)}, {truncate: 10, data: nil}},
	} {
		t.Run(name, func(t *testing.T) {
			if sum, ok := hashedChecksum(t, filepath.Join(t.TempDir(), "f.bin"), ops); ok {
				t.Errorf("recorded checksum %s, want none", sum)
			}
		})
	}
}

// TestChecksumsLookupRejectsChangedFile: a file rewritten, replaced or
// removed after its checksum was recorded has none.
func TestChecksumsLookupRejectsChangedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	write := func(sums *Checksums) {
		t.Helper()
		f, err := createHashedFile(path, sums)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("payload")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	stat := func() os.FileInfo {
		t.Helper()
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}

		return info
	}

	sums := newChecksums(testPiece)
	write(sums)
	if _, ok := sums.lookup(path, stat()); !ok {
		t.Fatal("no checksum after writing")
	}
	if err := os.WriteFile(path, []byte("PAYLOAD"), FilePerm); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Second)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
	if _, ok := sums.lookup(path, stat()); ok {
		t.Error("checksum of a file rewritten since")
	}

	write(sums)
	other := filepath.Join(dir, "other.bin")
	if err := os.WriteFile(other, []byte("payload"), FilePerm); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(other, path); err != nil {
		t.Fatal(err)
	}
	if _, ok := sums.lookup(path, stat()); ok {
		t.Error("checksum of a file replaced since")
	}

	write(sums)
	moved := filepath.Join(dir, "moved.bin")
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	sums.move(path, moved)
	info, err := os.Stat(moved)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sums.lookup(moved, info); !ok {
		t.Error("no checksum after a move")
	}

	var none *Checksums
	write(none)
	if _, ok := none.lookup(path, stat()); ok {
		t.Error("nil Checksums returned a checksum")
	}
}
