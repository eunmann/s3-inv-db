package extsort

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// prefixGen makes n prefixes from a seeded source.
type prefixGen func(r *rand.Rand, n int) []string

// pathSegments mixes long shared directory names with bytes that sort
// below '/' and multi-byte and invalid UTF-8.
var pathSegments = []string{
	"customer-data-warehouse", "customer-data-warehouse-v2", "customer.data", "customer!",
	"year=2024", "year=2025", "month=2024-01", "month=2024-1", "day=2024-01-15",
	"hour=2024-01-15-03", "hour=2024-01-15-03-", "a", "a-", "a.", "a!", "a/b",
	"日本語", "日本", "naïve", "naïve", "\xff\xfe", "\x00", "\x00\x01", "Z", "",
}

func randomPath(r *rand.Rand, minDepth, maxDepth int) string {
	var b strings.Builder
	for range minDepth + r.IntN(maxDepth-minDepth+1) {
		b.WriteString(pathSegments[r.IntN(len(pathSegments))])
		b.WriteByte('/')
	}
	if r.IntN(3) == 0 {
		fmt.Fprintf(&b, "part-%05d", r.IntN(100_000))
	}

	return b.String()
}

// byteAlphabet is few enough bytes that random strings share prefixes,
// around '/' and across the UTF-8 lead and continuation ranges.
const byteAlphabet = "\x00\x01!-./0Za\x7f\x80\xc3\xa9\xff"

var prefixGens = map[string]prefixGen{
	"deep_paths": func(r *rand.Rand, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = randomPath(r, 4, 12)
		}

		return out
	},
	"shared_root": func(r *rand.Rand, n int) []string {
		root := strings.Repeat("analytics-pipeline-outputs/", 6)
		out := make([]string, n)
		for i := range out {
			out[i] = root + randomPath(r, 0, 4)
		}

		return out
	},
	"random_bytes": func(r *rand.Rand, n int) []string {
		out := make([]string, n)
		for i := range out {
			b := make([]byte, r.IntN(20))
			for j := range b {
				b[j] = byteAlphabet[r.IntN(len(byteAlphabet))]
			}
			out[i] = string(b)
		}

		return out
	},
	"duplicates": func(r *rand.Rand, n int) []string {
		pool := make([]string, 1+n/50)
		for i := range pool {
			pool[i] = randomPath(r, 1, 8)
		}
		out := make([]string, n)
		for i := range out {
			out[i] = pool[r.IntN(len(pool))]
		}

		return out
	},
	"long_equal": func(r *rand.Rand, n int) []string {
		long := strings.Repeat("x/", 300)
		out := make([]string, n)
		for i := range out {
			switch r.IntN(4) {
			case 0:
				out[i] = long
			case 1:
				out[i] = long + "y"
			case 2:
				out[i] = long[:len(long)-1]
			default:
				out[i] = long[:r.IntN(len(long))]
			}
		}

		return out
	},
	"empty": func(_ *rand.Rand, n int) []string {
		return make([]string, n)
	},
}

// arrangements reorder generated prefixes before the sort.
var arrangements = map[string]func([]string){
	"random":  func([]string) {},
	"sorted":  func(s []string) { slices.Sort(s) },
	"reverse": func(s []string) { slices.Sort(s); slices.Reverse(s) },
	"organ_pipe": func(s []string) {
		slices.Sort(s)
		slices.Reverse(s[len(s)/2:])
	},
}

func TestSortPrefixRowsMatchesStringsCompare(t *testing.T) {
	sizes := []int{0, 1, 2, 3, 7, 16, 17, 33, 127, 128, 1000, 10_000, 100_000}
	var scratch []keyedRow
	for genName, gen := range prefixGens {
		for arrName, arrange := range arrangements {
			for _, n := range sizes {
				if n == 100_000 && arrName != "random" {
					continue
				}
				r := rand.New(rand.NewPCG(uint64(n), uint64(len(genName)+len(arrName))))
				prefixes := gen(r, n)
				arrange(prefixes)
				rows := makePrefixRows(prefixes)
				scratch = sortPrefixRows(rows, scratch)
				assertSortedPermutation(t, fmt.Sprintf("%s/%s/n=%d", genName, arrName, n), prefixes, rows)
			}
		}
	}
	for _, k := range scratch[:cap(scratch)] {
		if k.row != nil || k.prefix != "" || k.key != 0 {
			t.Fatal("returned sort scratch keeps a row")
		}
	}
}

// TestMultikeySortFallback forces the pdqsort fallback, at depth 0 and
// past a shared leading digit, and a small budget that runs out partway.
func TestMultikeySortFallback(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, tc := range []struct {
		name   string
		root   string
		depth  int
		budget int
	}{
		{"depth_0", "", 0, 0},
		{"depth_7", "shared/", digitBytes, 0},
		{"depth_14", "shared/shared/", 2 * digitBytes, 0},
		{"budget_1", "", 0, 1},
		{"budget_2", "", 0, 2},
	} {
		prefixes := prefixGens["deep_paths"](r, 5000)
		for i := range prefixes {
			prefixes[i] = tc.root + prefixes[i]
		}
		rows := makePrefixRows(prefixes)
		keyed := make([]keyedRow, len(rows))
		for i, row := range rows {
			keyed[i] = keyedRow{prefix: row.Prefix, row: row, key: digitKey(row.Prefix, tc.depth)}
		}
		multikeySort(keyed, tc.depth, tc.budget)
		for i := range keyed {
			rows[i] = keyed[i].row
		}
		assertSortedPermutation(t, tc.name, prefixes, rows)
	}
}

func TestCompareKeyedMatchesStringsCompare(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	gen := prefixGens["random_bytes"]
	for range 20_000 {
		pair := gen(r, 2)
		shared := gen(r, 1)[0]
		a, b := shared+pair[0], shared+pair[1]
		if r.IntN(4) == 0 {
			b = a + pair[1]
		}
		d := r.IntN(len(shared) + 1)
		ka := keyedRow{prefix: a, key: digitKey(a, d)}
		kb := keyedRow{prefix: b, key: digitKey(b, d)}
		if got, want := compareKeyed(ka, kb, d), strings.Compare(a, b); got != want {
			t.Fatalf("compareKeyed(%q, %q, %d) = %d, want %d", a, b, d, got, want)
		}
	}
}

func makePrefixRows(prefixes []string) []*PrefixRow {
	rows := make([]*PrefixRow, len(prefixes))
	for i, p := range prefixes {
		rows[i] = &PrefixRow{Prefix: p}
	}

	return rows
}

// assertSortedPermutation checks rows hold the rows made from prefixes,
// each once, in slices.SortFunc(strings.Compare) order.
func assertSortedPermutation(t *testing.T, name string, prefixes []string, rows []*PrefixRow) {
	t.Helper()
	want := slices.Clone(prefixes)
	slices.SortFunc(want, strings.Compare)
	seen := make(map[*PrefixRow]bool, len(rows))
	for i, row := range rows {
		if seen[row] {
			t.Fatalf("%s: row %q appears twice", name, row.Prefix)
		}
		seen[row] = true
		if row.Prefix != want[i] {
			t.Fatalf("%s: rows[%d] = %q, want %q", name, i, row.Prefix, want[i])
		}
	}
	if len(rows) != len(want) {
		t.Fatalf("%s: %d rows, want %d", name, len(rows), len(want))
	}
}
