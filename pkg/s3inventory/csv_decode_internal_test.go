package s3inventory

import (
	"net/url"
	"testing"
)

// decodeKeyCases cover each branch of appendUnescape: plain runs, '+',
// escapes of either case, '%2F' (a '/' the key really holds), escapes
// at the ends, and invalid or truncated escapes.
var decodeKeyCases = []string{
	"",
	"plain/key.txt",
	"a+b",
	"+",
	"%20",
	"%2F",
	"%2f",
	"dir%2Fname/file",
	"store/0%2C72%2C0%2C10/image_data/0.0.0",
	"%E2%9C%93/%e2%9c%93",
	"%2B%2b+%25",
	"trailing%",
	"trailing%4",
	"%zz",
	"%4g",
	"%g4",
	"ok%20then%",
	"bad%zz/key",
	"%%41",
	"a%2",
	"x+%41+y%42",
	"+%zz",
	"a+b%",
	"%2B+",
	"++%41%",
}

// checkDecodeKey compares decodeKey with url.QueryUnescape, the decoder
// it replaced: the same key for valid encoding, and the raw key counted
// as undecodable otherwise.
func checkDecodeKey(t *testing.T, raw string) {
	t.Helper()
	r := &CSVReader{}
	got := r.decodeKey(raw)
	want, err := url.QueryUnescape(raw)
	if err != nil {
		want = raw
	}
	if got != want {
		t.Errorf("decodeKey(%q) = %q, want %q", raw, got, want)
	}
	wantBad := uint64(0)
	if err != nil {
		wantBad = 1
	}
	if r.anomalies.UndecodableKeys != wantBad {
		t.Errorf("decodeKey(%q): UndecodableKeys = %d, want %d", raw, r.anomalies.UndecodableKeys, wantBad)
	}
}

func TestDecodeKeyMatchesQueryUnescape(t *testing.T) {
	for _, raw := range decodeKeyCases {
		checkDecodeKey(t, raw)
	}
}

func FuzzDecodeKeyMatchesQueryUnescape(f *testing.F) {
	for _, raw := range decodeKeyCases {
		f.Add(raw)
	}
	f.Fuzz(checkDecodeKey)
}

// TestDecodeKeyReusesBuffer checks that decoding keeps one buffer
// across keys and that each key reads right until the next decode.
func TestDecodeKeyReusesBuffer(t *testing.T) {
	r := &CSVReader{}
	keys := []struct{ raw, want string }{
		{"long%20directory%20name/file%2B1", "long directory name/file+1"},
		{"short+key", "short key"},
		{"plain", "plain"},
		{"a%2Cb/c", "a,b/c"},
	}
	for _, k := range keys {
		if got := r.decodeKey(k.raw); got != k.want {
			t.Fatalf("decodeKey(%q) = %q, want %q", k.raw, got, k.want)
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		for _, k := range keys {
			_ = r.decodeKey(k.raw)
		}
	})
	if allocs != 0 {
		t.Errorf("decodeKey allocated %.0f times per run, want 0", allocs)
	}
}
