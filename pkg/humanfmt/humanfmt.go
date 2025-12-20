// Package humanfmt provides human-readable formatting for bytes, durations, and throughput.
package humanfmt

import (
	"fmt"
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

// ThroughputUint64 is like Throughput but for uint64.
func ThroughputUint64(bytes uint64, d time.Duration) string {
	return Throughput(int64(bytes), d)
}

// Examples: "1.23M", "456K", "789".
func Count(n int64) string {
	if n < 0 {
		return strconv.FormatInt(n, 10)
	}

	const (
		thousand = 1000
		million  = 1000 * thousand
		billion  = 1000 * million
	)

	switch {
	case n >= billion:
		return fmt.Sprintf("%.2fB", float64(n)/billion)
	case n >= million:
		return fmt.Sprintf("%.2fM", float64(n)/million)
	case n >= thousand:
		return fmt.Sprintf("%.2fK", float64(n)/thousand)
	default:
		return strconv.FormatInt(n, 10)
	}
}

// CountUint64 is like Count but for uint64.
func CountUint64(n uint64) string {
	return Count(int64(n))
}
