package humanfmt_test

import (
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
)

func TestBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{100, "100 B"},
		{1023, "1023 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1048576, "1.00 MiB"},
		{1572864, "1.50 MiB"},
		{1073741824, "1.00 GiB"},
		{1610612736, "1.50 GiB"},
		{1099511627776, "1.00 TiB"},
		{1649267441664, "1.50 TiB"},
		{-100, "-100 B"},
	}

	for _, tt := range tests {
		got := humanfmt.Bytes(tt.input)
		if got != tt.want {
			t.Errorf("Bytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestDuration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, "0ns"},
		{500 * time.Nanosecond, "500ns"},
		{1 * time.Microsecond, "1.0µs"},
		{500 * time.Microsecond, "500.0µs"},
		{1 * time.Millisecond, "1.0ms"},
		{1500 * time.Millisecond, "1.50s"},
		{1 * time.Second, "1.00s"},
		{1230 * time.Millisecond, "1.23s"},
		{59 * time.Second, "59.00s"},
		{60 * time.Second, "1m"},
		{90 * time.Second, "1m30s"},
		{3600 * time.Second, "1h"},
		{3660 * time.Second, "1h1m"},
		{7200 * time.Second, "2h"},
		{8100 * time.Second, "2h15m"},
	}

	for _, tt := range tests {
		got := humanfmt.Duration(tt.input)
		if got != tt.want {
			t.Errorf("Duration(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCount(t *testing.T) {
	tests := []struct {
		name  string
		input int64
		want  string
	}{
		{"zero", 0, "0"},
		{"small", 42, "42"},
		{"three digits", 999, "999"},
		{"four digits → commas", 1234, "1.2K"}, // routed to K
		{"exact thousand", 1000, "1.0K"},
		{"round half away", 1250, "1.3K"},
		{"1.5K", 1500, "1.5K"},
		{"12.5K", 12_500, "12.5K"},
		{"1M", 1_000_000, "1.0M"},
		{"1.5M", 1_500_000, "1.5M"},
		{"1B", 1_000_000_000, "1.0B"},
		{"1.5B", 1_500_000_000, "1.5B"},
		{"negative", -100, "-100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := humanfmt.Count(tt.input)
			if got != tt.want {
				t.Errorf("Count(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatWithCommas(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{42, "42"},
		{999, "999"},
	}
	for _, tt := range tests {
		if got := humanfmt.FormatWithCommas(tt.n); got != tt.want {
			t.Errorf("FormatWithCommas(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestBytesUint64_OverflowAbove2_63(t *testing.T) {
	// 2^63 is above int64 max; the old int64-cast wrapper would have
	// underflowed to a negative value and returned "-... B". The
	// native uint64 implementation must surface a real EiB number
	// (2^63 ≈ 8 EiB).
	const aboveMax = uint64(1) << 63
	got := humanfmt.BytesUint64(aboveMax)
	if !strings.HasSuffix(got, "EiB") {
		t.Errorf("humanfmt.BytesUint64(2^63) = %q, want an EiB-suffix value", got)
	}
}

func TestBytes_PebiAndExbi(t *testing.T) {
	tests := []struct {
		name  string
		input int64
		want  string
	}{
		{"1 PiB", 1 << 50, "1.00 PiB"},
		{"1.5 PiB", (1 << 50) + (1 << 49), "1.50 PiB"},
		{"1024 TiB → 1 PiB", 1024 * (1 << 40), "1.00 PiB"},
		{"1 EiB", 1 << 60, "1.00 EiB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := humanfmt.Bytes(tt.input); got != tt.want {
				t.Errorf("Bytes(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestCountUint64_OverflowAbove2_63(t *testing.T) {
	const aboveMax = uint64(1) << 63
	got := humanfmt.CountUint64(aboveMax)
	if !strings.HasSuffix(got, "P") {
		t.Errorf("humanfmt.CountUint64(2^63) = %q, want a P-suffix value", got)
	}
}

func TestCount_TeraAndPeta(t *testing.T) {
	tests := []struct {
		name  string
		input int64
		want  string
	}{
		{"1T", 1_000_000_000_000, "1.0T"},
		{"2.5T", 2_500_000_000_000, "2.5T"},
		{"1P", 1_000_000_000_000_000, "1.0P"},
		{"3.7P", 3_700_000_000_000_000, "3.7P"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := humanfmt.Count(tt.input); got != tt.want {
				t.Errorf("Count(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestRunTimestamp(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"2026-05-13T03-02Z", "2026-05-13 03:02 UTC"},
		{"2026-05-13T03-02-17Z", "2026-05-13 03:02 UTC"},
		{"not-a-timestamp", "not-a-timestamp"},
		{"", ""},
	}
	for _, c := range cases {
		if got := humanfmt.RunTimestamp(c.in); got != c.want {
			t.Errorf("RunTimestamp(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
