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
)

// Bytes formats a byte count using IEC binary units (KiB, MiB, GiB, TiB).
// Returns a compact human-readable string like "1.23 GiB".
func Bytes(b int64) string {
	if b < 0 {
		return fmt.Sprintf("%d B", b)
	}

	switch {
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

// BytesUint64 is like Bytes but for uint64.
func BytesUint64(b uint64) string {
	return Bytes(int64(b))
}

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

// Throughput formats bytes per duration as a human-readable rate.
// Returns a string like "123.4 MiB/s".
func Throughput(bytes int64, d time.Duration) string {
	if d <= 0 {
		return "∞"
	}

	bytesPerSec := float64(bytes) / d.Seconds()

	switch {
	case bytesPerSec >= TiB:
		return fmt.Sprintf("%.2f TiB/s", bytesPerSec/TiB)
	case bytesPerSec >= GiB:
		return fmt.Sprintf("%.2f GiB/s", bytesPerSec/GiB)
	case bytesPerSec >= MiB:
		return fmt.Sprintf("%.2f MiB/s", bytesPerSec/MiB)
	case bytesPerSec >= KiB:
		return fmt.Sprintf("%.2f KiB/s", bytesPerSec/KiB)
	default:
		return fmt.Sprintf("%.0f B/s", bytesPerSec)
	}
}

// Count renders an integer for human display.
//
//   - < 0     → plain decimal
//   - 0–999   → plain decimal with grouping commas
//   - 1,000+  → "X.YK" / "X.YM" / "X.YB", one decimal, half-away-from-zero
//
// Examples: "789", "1.2K", "12.5K", "1.0M", "2.5B".
func Count(n int64) string {
	if n < 0 {
		return strconv.FormatInt(n, 10)
	}

	const (
		thousand = 1000.0
		million  = 1000 * thousand
		billion  = 1000 * million
	)

	f := float64(n)
	switch {
	case f >= billion:
		return fmt.Sprintf("%.1fB", math.Round(f/billion*10)/10)
	case f >= million:
		return fmt.Sprintf("%.1fM", math.Round(f/million*10)/10)
	case f >= thousand:
		return fmt.Sprintf("%.1fK", math.Round(f/thousand*10)/10)
	default:
		return formatWithCommas(n)
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

// CountUint64 is like Count but for uint64.
func CountUint64(n uint64) string {
	return Count(int64(n))
}
