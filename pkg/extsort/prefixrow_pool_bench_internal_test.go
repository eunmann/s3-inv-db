package extsort

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// serializeRunBody returns a binary blob shaped like the body of a run
// file: n records in sorted order, as runRecordDecoder reads them.
func serializeRunBody(n int) []byte {
	rows := make([]PrefixRow, n)
	for i := range rows {
		rows[i] = PrefixRow{
			Prefix:     fmt.Sprintf("tenant-%05d/year=2024/month=%02d/file.parquet", i/12, (i%12)+1),
			Depth:      3,
			Count:      uint64(i),
			TotalBytes: uint64(i * 4096),
			TierCounts: [MaxTiers]uint64{tiers.Standard: uint64(i)},
			TierBytes:  [MaxTiers]uint64{tiers.Standard: uint64(i) * 4096},
		}
	}
	var out []byte
	prev := ""
	for i := range rows {
		out = encodeRunRecord(out, prev, &rows[i])
		prev = rows[i].Prefix
	}

	return out
}

// BenchmarkReadPrefixRowReused decodes run-file records into a
// caller-owned PrefixRow.
func BenchmarkReadPrefixRowReused(b *testing.B) {
	const N = 10_000
	body := serializeRunBody(N)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d := runRecordDecoder{in: readWindow{r: bufio.NewReaderSize(bytes.NewReader(body), mergeReaderBufferSize)}}
		row := &PrefixRow{}
		for {
			err := d.next(row)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
