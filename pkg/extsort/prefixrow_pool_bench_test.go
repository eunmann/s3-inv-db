package extsort

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// serializeRunBody returns a binary blob shaped like the body of a run
// file — N concatenated records in the format readPrefixRowRecord
// understands. Used by the readPrefixRow benches below.
func serializeRunBody(n int) []byte {
	buf := new(bytes.Buffer)
	w := &runRecordEncoder{buf: buf}
	for i := range n {
		w.write(&PrefixRow{
			Prefix:     fmt.Sprintf("tenant-%05d/year=2024/month=%02d/file.parquet", i%1000, (i%12)+1),
			Depth:      3,
			Count:      uint64(i),
			TotalBytes: uint64(i * 4096),
			TierCounts: [MaxTiers]uint64{tiers.Standard: uint64(i)},
			TierBytes:  [MaxTiers]uint64{tiers.Standard: uint64(i) * 4096},
		})
	}
	return buf.Bytes()
}

type runRecordEncoder struct {
	buf *bytes.Buffer
	scratch [1024]byte
}

func (e *runRecordEncoder) write(row *PrefixRow) {
	rfw := &RunFileWriter{buf: e.scratch[:]}
	_ = rfw // placeholder reuse if needed
	// Hand-encode using the same layout as RunFileWriter.Write.
	prefixLen := len(row.Prefix)
	e.buf.Write(u32le(uint32(prefixLen)))
	e.buf.WriteString(row.Prefix)
	e.buf.Write(u16le(row.Depth))
	e.buf.Write(u64le(row.Count))
	e.buf.Write(u64le(row.TotalBytes))
	for _, v := range row.TierCounts {
		e.buf.Write(u64le(v))
	}
	for _, v := range row.TierBytes {
		e.buf.Write(u64le(v))
	}
}

func u16le(v uint16) []byte { return []byte{byte(v), byte(v >> 8)} }
func u32le(v uint32) []byte { return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)} }
func u64le(v uint64) []byte {
	return []byte{
		byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24),
		byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56),
	}
}

// BenchmarkReadPrefixRowAlloc reads run-file records via the path that
// allocates a fresh PrefixRow per call (Read).
func BenchmarkReadPrefixRowAlloc(b *testing.B) {
	const N = 10_000
	body := serializeRunBody(N)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := bytes.NewReader(body)
		buf := make([]byte, 1024)
		for {
			row, err := readPrefixRowRecord(r, &buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			_ = row
		}
	}
}

// BenchmarkReadPrefixRowReused reads run-file records via ReadInto into
// a caller-owned PrefixRow.
func BenchmarkReadPrefixRowReused(b *testing.B) {
	const N = 10_000
	body := serializeRunBody(N)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := bytes.NewReader(body)
		buf := make([]byte, 1024)
		row := &PrefixRow{}
		for {
			_, err := readPrefixRowRecordInto(r, &buf, row)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
