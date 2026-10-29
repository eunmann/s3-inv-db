package s3inventory

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"net/url"
	"slices"
	"strconv"
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

// lineBufferSizes are a buffer small enough to force the long-line
// handoff to encoding/csv, and the real one.
var lineBufferSizes = []int{16, csvLineBuffer}

// checkRecordsMatch: the CSVReader returns exactly encoding/csv's
// records (and fails exactly when it does), whether lines are split in
// place or handed to encoding/csv, at every line buffer size. Lines
// split in place keep only their first maxFields fields, so records
// are compared up to that many.
func checkRecordsMatch(t *testing.T, data []byte) {
	t.Helper()
	want, wantErr := readAll(newFallbackCSV(bytes.NewReader(data)).Read)
	for _, size := range lineBufferSizes {
		for _, maxFields := range []int{1, 3, math.MaxInt} {
			r := &CSVReader{lines: bufio.NewReaderSize(bytes.NewReader(data), size), maxFields: maxFields}
			got, gotErr := readAll(r.record)
			kept := func(rec []string) []string { return rec[:min(len(rec), maxFields)] }
			same := func(g, w []string) bool { return slices.Equal(kept(g), kept(w)) }
			if gotErr != wantErr || !slices.EqualFunc(got, want, same) {
				t.Fatalf("buffer %d, %d fields, input %q:\n got %q (error %v)\nwant %q (error %v)",
					size, maxFields, data, got, gotErr, want, wantErr)
			}
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
		",,,\n",
		`"","",""` + "\n",
		`a,,"",b,` + "\n",
		`"a",` + "\n",
		`a,"b",c,"d"` + "\n" + `"a",b,"c",d` + "\n",
		"no newline at end",
		"cr at end\r",
		"inner\rcr,x\n",
		`"a""b",c` + "\n",
		`"a"b,c` + "\n",
		`a"b,c` + "\n",
		`a,b"c` + "\n",
		`a,b,"` + "\n",
		`"a,b",c` + "\n",
		`"multi` + "\n" + `line",x` + "\nnext,row\n",
		`"unterminated` + "\n",
		" \"lead space\",x\n",
		"a,\"\"\n",
		"plain,then\n\"q\",\"r\"\nx\"y,z\nafter,fallback\n",
		"averyveryverylonglinethatexceedsthesmallbuffer,with,fields\nshort,line\n",
		// Past the third field: trailing columns, then a quoted comma,
		// an escaped quote, a stray quote and a field running over the
		// line end, each after fields that split in place.
		"b,k,1,S,t1,t2,t3,t4\nb,k,2,S\n",
		`"b","k","1","S","x,y",z` + "\n" + `"b","k2","2","S"` + "\n",
		`"b","k","1","S","x""y"` + "\nb,k2,2,S\n",
		`b,k,1,S,a"b` + "\nb,k2,2,S\n",
		`b,k,1,S,"open` + "\n" + `still",x` + "\nb,k2,2,S\n",
	} {
		checkRecordsMatch(t, []byte(in))
	}
}

// A line split in place keeps only its first maxFields fields, but the
// rest must still be plain or wholly quoted for it to end the record.
func TestSplitPlainCSVKeepsMaxFields(t *testing.T) {
	for _, c := range []struct {
		line string
		want []string
		ok   bool
	}{
		{"b,k,1,S,t1,t2", []string{"b", "k", "1"}, true},
		{`"b","k","1","S","t1"`, []string{"b", "k", "1"}, true},
		{`"b",k,"1",S,"t1",t2`, []string{"b", "k", "1"}, true},
		{"b,k", []string{"b", "k"}, true},
		{`"b","k",`, []string{"b", "k", ""}, true},
		{`b,k,1,S,"x,y",z`, []string{"b", "k", "1"}, true},
		{`b,k,1,S,"x""y"`, nil, false},
		{`b,k,1,S,x"y`, nil, false},
		{`b,k,1,S,"open`, nil, false},
	} {
		got, ok := splitPlainCSV([]byte(c.line), nil, 3)
		if ok != c.ok || !slices.Equal(got, c.want) {
			t.Errorf("splitPlainCSV(%q, 3) = %q, %v; want %q, %v", c.line, got, ok, c.want, c.ok)
		}
	}
}

func FuzzCSVRecordsMatchEncodingCSV(f *testing.F) {
	for _, s := range []string{"a,b\n", `"a","b"` + "\r\n", `"a""b"` + "\n", "x\"y\n", "\"m\nl\",z\n", `a,b,c,"d` + "\ne\",f\n"} {
		f.Add([]byte(s))
	}
	f.Fuzz(checkRecordsMatch)
}

// rowConfigs are a current-version layout and a versioned one whose
// delete-marker and access-tier columns sit around the others.
var rowConfigs = []CSVReaderConfig{
	{KeyCol: 1, SizeCol: 2, StorageCol: 3, AccessTierCol: -1, DeleteMarkerCol: -1},
	{KeyCol: 0, SizeCol: 1, StorageCol: -1, AccessTierCol: 3, DeleteMarkerCol: 2},
}

// referenceRows reads data as the CSVReader did before it split lines
// in place: encoding/csv records, url.QueryUnescape keys, and
// strconv.ParseUint sizes. It reports whether reading stopped on an
// error.
func referenceRows(data []byte, cfg CSVReaderConfig) ([]Row, Anomalies, bool) {
	csvr := newFallbackCSV(bytes.NewReader(data))
	var rows []Row
	var a Anomalies
	field := func(rec []string, col int) string {
		if col >= 0 && len(rec) > col {
			return strings.Clone(rec[col])
		}

		return ""
	}
	for {
		rec, err := csvr.Read()
		if errors.Is(err, io.EOF) {
			return rows, a, false
		}
		if err != nil {
			return rows, a, true
		}
		if len(rec) <= cfg.KeyCol || len(rec) <= cfg.SizeCol {
			a.ShortRows++

			continue
		}
		if strings.EqualFold(strings.TrimSpace(field(rec, cfg.DeleteMarkerCol)), "true") {
			continue
		}
		key, err := url.QueryUnescape(rec[cfg.KeyCol])
		if err != nil {
			a.UndecodableKeys++
			key = rec[cfg.KeyCol]
		}
		if key == "" {
			a.MissingKeys++

			continue
		}
		size, err := strconv.ParseUint(strings.TrimSpace(rec[cfg.SizeCol]), 10, 64)
		if err != nil {
			a.BadSizes++
			size = 0
		}
		rows = append(rows, Row{
			Key:          strings.Clone(key),
			Size:         size,
			StorageClass: field(rec, cfg.StorageCol),
			AccessTier:   field(rec, cfg.AccessTierCol),
		})
	}
}

// checkRowsMatch: Next returns referenceRows' rows and anomalies for
// each layout at each line buffer size.
func checkRowsMatch(t *testing.T, data []byte) {
	t.Helper()
	for _, cfg := range rowConfigs {
		want, wantAnomalies, wantErr := referenceRows(data, cfg)
		for _, size := range lineBufferSizes {
			r := newCSVReader(bufio.NewReaderSize(bytes.NewReader(data), size), cfg)
			var got []Row
			gotErr := false
			for {
				row, err := r.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					gotErr = true

					break
				}
				row.Key = strings.Clone(row.Key)
				row.StorageClass = strings.Clone(row.StorageClass)
				row.AccessTier = strings.Clone(row.AccessTier)
				got = append(got, row)
			}
			if gotErr != wantErr || !slices.Equal(got, want) || r.Anomalies() != wantAnomalies {
				t.Fatalf("config %+v, buffer %d, input %q:\n got %+v %+v (error %v)\nwant %+v %+v (error %v)",
					cfg, size, data, got, r.Anomalies(), gotErr, want, wantAnomalies, wantErr)
			}
		}
	}
}

// rowCases cover the fields Next reads: keys URL encoded, quoted, with
// commas, escaped quotes or newlines; empty, padded, overlong and
// malformed sizes; delete markers; short rows; and trailing columns
// before and after the ones read.
var rowCases = []string{
	`"b","a%2Cb+c%2Fd","12","STANDARD",""` + "\n",
	`"b","a,b/c","1","STANDARD"` + "\n",
	`"b","a""b","1","S"` + "\n",
	`"b","multi` + "\n" + `line","1","S"` + "\n" + `"b","k2","2","S"` + "\n",
	"b,bad%zz,5,S\nb,%E2%9C%93/x,6,GLACIER\nb,+lead+,7,S\n",
	"b,,1,S\nb,k,,S\n,,,\n\"\",\"\",\"\",\"\"\nb,k,1,\n",
	"b, 12 ,S\nb,0012,S\nb,9999999999999999999,S\nb,18446744073709551615,S\nb,18446744073709551616,S\n",
	"b,+5,S\nb,-1,S\nb,1_000,S\nb,0x10,S\nb,1e3,S\nb,٣,S\n",
	"b\nb,k\nb,k,1\n",
	"k,5,true,X\nk,5,TRUE ,X\nk,5,false,FREQUENT\nk,5\n",
	"b,k,1,S,t1,t2,t3,t4\n",
	`"b","k","1","S","x,y","x""y"` + "\n" + `b,k2,2,S,"open` + "\n" + `still",x` + "\nb,k3,3,S\n",
}

func TestCSVRowsMatchReference(t *testing.T) {
	for _, in := range rowCases {
		checkRowsMatch(t, []byte(in))
	}
}

func FuzzCSVRowsMatchReference(f *testing.F) {
	for _, in := range rowCases {
		f.Add([]byte(in))
	}
	f.Fuzz(checkRowsMatch)
}

func TestParseDigitsMatchesParseUint(t *testing.T) {
	for _, s := range []string{
		"", "0", "7", "0012", "1024", "9999999999999999999", "10000000000000000000",
		"18446744073709551615", "18446744073709551616", " 1", "1 ", "+1", "-1", "1_0", "0x1", "1.0", "a", "٣",
	} {
		checkParseDigits(t, s)
	}
}

func FuzzParseDigitsMatchesParseUint(f *testing.F) {
	for _, s := range []string{"", "0", "123", "9999999999999999999", "18446744073709551616", "1a"} {
		f.Add(s)
	}
	f.Fuzz(checkParseDigits)
}

// checkParseDigits: parseDigits returns what strconv.ParseUint does for
// every string of 1 to 19 ASCII digits, and declines every other.
func checkParseDigits(t *testing.T, s string) {
	t.Helper()
	got, ok := parseDigits(s)
	digits := s != "" && len(s) <= maxSafeDigits && strings.Trim(s, "0123456789") == ""
	if ok != digits {
		t.Fatalf("parseDigits(%q) ok = %v, want %v", s, ok, digits)
	}
	if !ok {
		return
	}
	if want, err := strconv.ParseUint(s, 10, 64); err != nil || got != want {
		t.Fatalf("parseDigits(%q) = %d, ParseUint = %d, %v", s, got, want, err)
	}
}
