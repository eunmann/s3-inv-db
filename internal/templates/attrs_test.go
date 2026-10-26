package templates_test

import (
	"net/url"
	"strconv"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/templates"
)

// textValue marshals itself as text, which json.Marshal honours.
type textValue struct{ v string }

func (t textValue) MarshalText() ([]byte, error) { return []byte("t:" + t.v), nil }

var adversarialValues = []string{
	"",
	"plain/prefix/",
	`<script>alert("x")</script>`,
	`a&b<c>d"e\f`,
	"tab\tnl\nret\rbs\bff\f",
	"\x00\x01\x1f\x7f",
	"bad\xffutf8",
	"trunc\xc3",
	"surrogate\xed\xa0\x80",
	"sep para ",
	"emoji 🌍 ünïcode",
	"a b#c?d/e%f+g=h&i",
	"src/inv/2026-01-01T00-00Z",
}

func parityCases() [][]any {
	fixed := [][]any{
		{},
		{"inventory_id", "inv-1", "prefix", "a/b/"},
		{"from", namedID("src/a"), "to", namedID("src/b"), "prefix", "x/"},
		{"inventory_id", "inv", "prefix", "p/", "sort", "size", "dir", "desc", "page", 7},
		{"page", -3, "flag", true, "off", false},
		{"z", "1", "a", "2", "m", "3"},
		{"dup", "first", "dup", "second"},
		{"dup", "first", "dup", ""},
		{"nil", nil},
		{"f", 1.5},
		{"text", textValue{"v"}},
		{"ptr", new(string)},
		{"k1", "1", "k2", "2", "k3", "3", "k4", "4", "k5", "5", "k6", "6", "k7", "7", "k8", "8", "k9", "9"},
		{"", "empty key"},
	}
	cases := make([][]any, 0, len(fixed)+3*len(adversarialValues))
	cases = append(cases, fixed...)
	for _, v := range adversarialValues {
		cases = append(cases,
			[]any{"prefix", v, "inventory_id", "inv"},
			[]any{v, "as key"},
			[]any{"id", namedID(v)},
		)
	}

	return cases
}

func TestHxValsJSONMatchesGeneralEncoder(t *testing.T) {
	for i, pairs := range parityCases() {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			got, err := templates.HxValsJSON(pairs...)
			if err != nil {
				t.Fatalf("hxValsJSON(%q): %v", pairs, err)
			}
			want, err := templates.HxValsJSONGeneral(pairs...)
			if err != nil {
				t.Fatalf("general(%q): %v", pairs, err)
			}
			if got != want {
				t.Errorf("hxValsJSON(%q)\n got %q\nwant %q", pairs, got, want)
			}
		})
	}
}

func TestBrowseURLMatchesGeneralEncoder(t *testing.T) {
	for i, pairs := range parityCases() {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			got, err := templates.BrowseURL(pairs...)
			if err != nil {
				t.Fatalf("browseURL(%q): %v", pairs, err)
			}
			if want := templates.BrowseURLGeneral(pairs...); got != want {
				t.Errorf("browseURL(%q)\n got %q\nwant %q", pairs, got, want)
			}
		})
	}
}

func TestRowLinksMatchGeneralEncoders(t *testing.T) {
	for _, a := range adversarialValues {
		for _, b := range adversarialValues {
			checkRowLinks(t, a, b, a+b)
		}
	}
}

// checkRowLinks checks BrowseLink and CompareLink against url.Values.
func checkRowLinks(t *testing.T, id, other, prefix string) {
	t.Helper()
	for _, size := range []string{"", "7"} {
		browse := queryURL("/browse", map[string]string{"inventory_id": id, "prefix": prefix, "page_size": size})
		if got := templates.BrowseLink(id, prefix, size); got != browse {
			t.Fatalf("BrowseLink(%q, %q, %q)\n got %q\nwant %q", id, prefix, size, got, browse)
		}
		for _, show := range []bool{false, true} {
			params := map[string]string{"from": id, "to": other, "prefix": prefix, "page_size": size}
			if show {
				params["show_unchanged"] = "true"
			}
			want := queryURL("/compare", params)
			if got := templates.CompareLink(id, other, prefix, size, show); got != want {
				t.Fatalf("CompareLink(%q, %q, %q, %q, %v)\n got %q\nwant %q", id, other, prefix, size, show, got, want)
			}
		}
	}
}

// queryURL is path with the url.Values query of params' non-empty
// values.
func queryURL(path string, params map[string]string) string {
	q := url.Values{}
	for k, v := range params {
		if v != "" {
			q.Set(k, v)
		}
	}
	if len(q) == 0 {
		return path
	}

	return path + "?" + q.Encode()
}

func FuzzAttrEncodersMatchGeneral(f *testing.F) {
	for _, v := range adversarialValues {
		f.Add("prefix", v, "inventory_id", v, 3)
	}
	f.Fuzz(func(t *testing.T, k1, v1, k2, v2 string, n int) {
		checkRowLinks(t, v1, v2, k1)
		for _, pairs := range [][]any{
			{k1, v1, k2, v2},
			{k1, v1, k2, n},
			{k1, namedID(v1), k2, v2, "page", n},
		} {
			got, err := templates.HxValsJSON(pairs...)
			if err != nil {
				t.Fatal(err)
			}
			want, err := templates.HxValsJSONGeneral(pairs...)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("hxValsJSON(%q)\n got %q\nwant %q", pairs, got, want)
			}
			gotURL, err := templates.BrowseURL(pairs...)
			if err != nil {
				t.Fatal(err)
			}
			if wantURL := templates.BrowseURLGeneral(pairs...); gotURL != wantURL {
				t.Fatalf("browseURL(%q)\n got %q\nwant %q", pairs, gotURL, wantURL)
			}
		}
	})
}
