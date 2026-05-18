// Package humanfmt provides human-readable formatting for bytes, durations, and throughput.
package humanfmt

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// Binary (IEC) units for bytes.
const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
	PiB = 1024 * TiB
	EiB = 1024 * PiB
)

// Bytes formats a byte count using IEC binary units (KiB, MiB, GiB, TiB).
// Returns a compact human-readable string like "1.23 GiB".
func Bytes(b int64) string {
	if b < 0 {
		return fmt.Sprintf("%d B", b)
	}

	return formatBytes(uint64(b))
}

// BytesUint64 is like Bytes but for uint64. Computes natively rather
// than casting through int64 so values above 2^63 don't underflow.
func BytesUint64(b uint64) string {
	return formatBytes(b)
}

func formatBytes(b uint64) string {
	switch {
	case b >= EiB:
		return fmt.Sprintf("%.2f EiB", float64(b)/EiB)
	case b >= PiB:
		return fmt.Sprintf("%.2f PiB", float64(b)/PiB)
	case b >= TiB:
		return fmt.Sprintf("%.2f TiB", float64(b)/TiB)
	case b >= GiB:
		return fmt.Sprintf("%.2f GiB", float64(b)/GiB)
	case b >= MiB:
		return fmt.Sprintf("%.2f MiB", float64(b)/MiB)
	case b >= KiB:
		return fmt.Sprintf("%.2f KiB", float64(b)/KiB)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// Duration formats a time.Duration using compact human-readable units.
// Examples: "1.23s", "45.6ms", "789µs", "1m30s", "2h15m".
func Duration(d time.Duration) string {
	if d < 0 {
		return d.String()
	}

	switch {
	case d >= time.Hour:
		h := d / time.Hour
		m := (d % time.Hour) / time.Minute
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}

		return fmt.Sprintf("%dh%dm", h, m)
	case d >= time.Minute:
		m := d / time.Minute
		s := (d % time.Minute) / time.Second
		if s == 0 {
			return fmt.Sprintf("%dm", m)
		}

		return fmt.Sprintf("%dm%ds", m, s)
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	case d >= time.Microsecond:
		return fmt.Sprintf("%.1fµs", float64(d)/float64(time.Microsecond))
	default:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
}

// Count renders an integer for human display.
//
//   - < 0     → plain decimal
//   - 0–999   → plain decimal with grouping commas
//   - 1,000+  → "X.YK" / "X.YM" / "X.YB" / "X.YT" / "X.YP", one decimal,
//     half-away-from-zero
//
// Examples: "789", "1.2K", "12.5K", "1.0M", "2.5B", "3.7T", "1.0P".
func Count(n int64) string {
	if n < 0 {
		return strconv.FormatInt(n, 10)
	}
	if n < kCutoff {
		return formatWithCommas(n)
	}

	return formatLargeCount(uint64(n))
}

const (
	kCutoff = 1000
	mCutoff = 1000 * kCutoff
	bCutoff = 1000 * mCutoff
	tCutoff = 1000 * bCutoff
	pCutoff = 1000 * tCutoff
)

func formatLargeCount(n uint64) string {
	f := float64(n)
	switch {
	case f >= pCutoff:
		return fmt.Sprintf("%.1fP", math.Round(f/pCutoff*10)/10)
	case f >= tCutoff:
		return fmt.Sprintf("%.1fT", math.Round(f/tCutoff*10)/10)
	case f >= bCutoff:
		return fmt.Sprintf("%.1fB", math.Round(f/bCutoff*10)/10)
	case f >= mCutoff:
		return fmt.Sprintf("%.1fM", math.Round(f/mCutoff*10)/10)
	default:
		return fmt.Sprintf("%.1fK", math.Round(f/kCutoff*10)/10)
	}
}

// formatWithCommas renders a non-negative int64 with thousands separators.
func formatWithCommas(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	first := len(s) % 3
	if first == 0 {
		first = 3
	}
	out := make([]byte, 0, len(s)+(len(s)-1)/3)
	out = append(out, s[:first]...)
	for i := first; i < len(s); i += 3 {
		out = append(out, ',')
		out = append(out, s[i:i+3]...)
	}

	return string(out)
}

// RunTimestamp reformats an S3-Inventory run folder name into a more
// readable label. AWS writes runs as "YYYY-MM-DDTHH-MMZ" (or with
// trailing seconds); humans read "YYYY-MM-DD HH:MM UTC" much faster.
// Falls back to the input unchanged when it doesn't match either
// layout — defensive for ad-hoc inventory names.
func RunTimestamp(raw string) string {
	for _, layout := range []string{"2006-01-02T15-04-05Z", "2006-01-02T15-04Z"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC().Format("2006-01-02 15:04 UTC")
		}
	}

	return raw
}

// CountUint64 is like Count but for uint64. Computes natively rather
// than casting through int64 so values above 2^63 don't underflow.
func CountUint64(n uint64) string {
	if n < kCutoff {
		return strconv.FormatUint(n, 10)
	}

	return formatLargeCount(n)
}
