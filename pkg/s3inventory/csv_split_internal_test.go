package s3inventory

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

// readAll returns a deep copy of every record read returns (the
// CSVReader's fields view its buffer until the next call), and whether
// reading stopped on an error other than io.EOF.
func readAll(read func() ([]string, error)) ([][]string, bool) {
	var out [][]string
	for {
		rec, err := read()
		if errors.Is(err, io.EOF) {
			return out, false
		}
		if err != nil {
			return out, true
		}
		rec = slices.Clone(rec)
		for i := range rec {
			rec[i] = strings.Clone(rec[i])
		}
		out = append(out, rec)
	}
}

// checkRecordsMatch: the CSVReader returns exactly encoding/csv's
// records (and fails exactly when it does), whether lines are split in
// place or handed to encoding/csv, at a buffer small enough to force
// the long-line handoff and at the real one.
func checkRecordsMatch(t *testing.T, data []byte) {
	t.Helper()
	want, wantErr := readAll(newFallbackCSV(bytes.NewReader(data)).Read)
	for _, size := range []int{16, csvLineBuffer} {
		r := &CSVReader{lines: bufio.NewReaderSize(bytes.NewReader(data), size)}
		got, gotErr := readAll(r.record)
		if gotErr != wantErr || !slices.EqualFunc(got, want, slices.Equal) {
			t.Fatalf("buffer %d, input %q:\n got %q (error %v)\nwant %q (error %v)", size, data, got, gotErr, want, wantErr)
		}
	}
}

func TestCSVRecordsMatchEncodingCSV(t *testing.T) {
	for _, in := range []string{
		"",
		"\n\n",
		"a,b,c\n",
		`"bucket","key%2Fx","12","STANDARD",""` + "\r\n" + `"b","k2","3","GLACIER","x"` + "\n",
		"a,b,\n,,\n",
		"no newline at end",
		"cr at end\r",
		"inner\rcr,x\n",
		`"a""b",c` + "\n",
		`"a"b,c` + "\n",
		`a"b,c` + "\n",
		`"multi` + "\n" + `line",x` + "\nnext,row\n",
		`"unterminated` + "\n",
		" \"lead space\",x\n",
		"a,\"\"\n",
		"plain,then\n\"q\",\"r\"\nx\"y,z\nafter,fallback\n",
		"averyveryverylonglinethatexceedsthesmallbuffer,with,fields\nshort,line\n",
	} {
		checkRecordsMatch(t, []byte(in))
	}
}

func FuzzCSVRecordsMatchEncodingCSV(f *testing.F) {
	for _, s := range []string{"a,b\n", `"a","b"` + "\r\n", `"a""b"` + "\n", "x\"y\n", "\"m\nl\",z\n"} {
		f.Add([]byte(s))
	}
	f.Fuzz(checkRecordsMatch)
}
