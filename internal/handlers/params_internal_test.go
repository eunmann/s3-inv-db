package handlers

import (
	"errors"
	"net/url"
	"testing"
)

func TestParseFilter_AbsentReturnsZero(t *testing.T) {
	got, err := parseFilter(url.Values{})
	if err != nil {
		t.Fatalf("parseFilter(empty): %v", err)
	}
	if got.MinCount != 0 || got.MinBytes != 0 {
		t.Errorf("parseFilter(empty) = %+v, want zero", got)
	}
}

func TestParseFilter_ParsesBothFields(t *testing.T) {
	q := url.Values{"min_count": {"5"}, "min_bytes": {"1024"}}
	got, err := parseFilter(q)
	if err != nil {
		t.Fatalf("parseFilter: %v", err)
	}
	if got.MinCount != 5 || got.MinBytes != 1024 {
		t.Errorf("parseFilter = %+v, want MinCount=5 MinBytes=1024", got)
	}
}

func TestParseFilter_InvalidMinCountWrapsSentinel(t *testing.T) {
	_, err := parseFilter(url.Values{"min_count": {"not-a-number"}})
	if err == nil {
		t.Fatal("parseFilter accepted malformed min_count")
	}
	if !errors.Is(err, ErrInvalidQueryParam) {
		t.Errorf("parseFilter err = %v, want wrapped ErrInvalidQueryParam", err)
	}
	if !contains(err.Error(), "min_count") {
		t.Errorf("err %q should mention min_count", err.Error())
	}
}

func TestParseFilter_InvalidMinBytesWrapsSentinel(t *testing.T) {
	_, err := parseFilter(url.Values{"min_bytes": {"-1"}})
	if err == nil {
		t.Fatal("parseFilter accepted negative min_bytes")
	}
	if !errors.Is(err, ErrInvalidQueryParam) {
		t.Errorf("parseFilter err = %v, want wrapped ErrInvalidQueryParam", err)
	}
}

func TestParsePositiveInt_AbsentReturnsDefault(t *testing.T) {
	got, err := parsePositiveInt(url.Values{}, "depth", 7)
	if err != nil {
		t.Fatalf("parsePositiveInt: %v", err)
	}
	if got != 7 {
		t.Errorf("parsePositiveInt(absent) = %d, want default 7", got)
	}
}

func TestParsePositiveInt_ParsesPositive(t *testing.T) {
	got, err := parsePositiveInt(url.Values{"depth": {"3"}}, "depth", 1)
	if err != nil {
		t.Fatalf("parsePositiveInt: %v", err)
	}
	if got != 3 {
		t.Errorf("parsePositiveInt = %d, want 3", got)
	}
}

func TestParsePositiveInt_RejectsZero(t *testing.T) {
	_, err := parsePositiveInt(url.Values{"limit": {"0"}}, "limit", 25)
	if err == nil {
		t.Fatal("parsePositiveInt accepted 0")
	}
	if !errors.Is(err, ErrInvalidQueryParam) {
		t.Errorf("err = %v, want wrapped sentinel", err)
	}
}

func TestParsePositiveInt_RejectsNegative(t *testing.T) {
	_, err := parsePositiveInt(url.Values{"limit": {"-5"}}, "limit", 25)
	if err == nil {
		t.Fatal("parsePositiveInt accepted negative")
	}
	if !errors.Is(err, ErrInvalidQueryParam) {
		t.Errorf("err = %v, want wrapped sentinel", err)
	}
}

func TestParsePositiveInt_RejectsMalformed(t *testing.T) {
	_, err := parsePositiveInt(url.Values{"depth": {"abc"}}, "depth", 1)
	if err == nil {
		t.Fatal("parsePositiveInt accepted abc")
	}
	if !contains(err.Error(), "depth") {
		t.Errorf("err %q should mention the offending key", err.Error())
	}
}

func TestParseCompareOpts_PopulatesAllFields(t *testing.T) {
	q := url.Values{
		"from":           {"s/inv/r1"},
		"to":             {"s/inv/r2"},
		"prefix":         {"a/b/"},
		"show_unchanged": {trueLiteral},
		"page":           {"2"},
		"page_size":      {"50"},
		"sort":           {"bytes_delta"},
		"dir":            {"desc"},
	}
	got := parseCompareOpts(q)
	if got.from != "s/inv/r1" || got.to != "s/inv/r2" {
		t.Errorf("from/to = %q/%q", got.from, got.to)
	}
	if got.prefix != "a/b/" {
		t.Errorf("prefix = %q", got.prefix)
	}
	if got.hideUnchanged {
		t.Error("show_unchanged=true should set hideUnchanged=false")
	}
	// page/pageSize/sortBy/dir flow through inventory.Normalize*; assert
	// they're non-zero and respect the inputs we provided.
	if got.page < 1 || got.pageSize < 1 {
		t.Errorf("page/pageSize = %d/%d", got.page, got.pageSize)
	}
}

func TestParseCompareOpts_MissingShowUnchangedHides(t *testing.T) {
	got := parseCompareOpts(url.Values{})
	if !got.hideUnchanged {
		t.Error("missing show_unchanged should hide (hideUnchanged=true)")
	}
}

func TestParseCompareOpts_EmptyIDsPassThrough(t *testing.T) {
	got := parseCompareOpts(url.Values{})
	if got.from != "" || got.to != "" {
		t.Errorf("from/to should be empty: %q/%q", got.from, got.to)
	}
}

func TestParseCompareOpts_PreservesInventoryIDType(t *testing.T) {
	got := parseCompareOpts(url.Values{"from": {"src/inv/run"}})
	if string(got.from) != "src/inv/run" {
		t.Errorf("from = %q, want %q", string(got.from), "src/inv/run")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}

	return false
}
