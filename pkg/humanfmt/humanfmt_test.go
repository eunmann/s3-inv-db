package humanfmt

import (
	"testing"
	"time"
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
		got := Bytes(tt.input)
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
		got := Duration(tt.input)
		if got != tt.want {
			t.Errorf("Duration(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestThroughput(t *testing.T) {
	tests := []struct {
		bytes    int64
		duration time.Duration
		want     string
	}{
		{0, time.Second, "0 B/s"},
		{1000, time.Second, "1000 B/s"},
		{1024, time.Second, "1.00 KiB/s"},
		{1048576, time.Second, "1.00 MiB/s"},
		{104857600, time.Second, "100.00 MiB/s"},
		{1073741824, time.Second, "1.00 GiB/s"},
		{1099511627776, time.Second, "1.00 TiB/s"},
		{1048576, 2 * time.Second, "512.00 KiB/s"},
		{0, 0, "∞"},
	}

	for _, tt := range tests {
		got := Throughput(tt.bytes, tt.duration)
		if got != tt.want {
			t.Errorf("Throughput(%d, %v) = %q, want %q", tt.bytes, tt.duration, got, tt.want)
		}
	}
}

func TestCount(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1.00K"},
		{1500, "1.50K"},
		{1000000, "1.00M"},
		{1500000, "1.50M"},
		{1000000000, "1.00B"},
		{1500000000, "1.50B"},
		{-100, "-100"},
	}

	for _, tt := range tests {
		got := Count(tt.input)
		if got != tt.want {
			t.Errorf("Count(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func BenchmarkBytes(b *testing.B) {
	sizes := []int64{100, 1024, 1048576, 1073741824}
	b.ResetTimer()
	for i := range b.N {
		_ = Bytes(sizes[i%len(sizes)])
	}
}

func BenchmarkDuration(b *testing.B) {
	durations := []time.Duration{
		100 * time.Microsecond,
		10 * time.Millisecond,
		1500 * time.Millisecond,
		90 * time.Second,
	}
	b.ResetTimer()
	for i := range b.N {
		_ = Duration(durations[i%len(durations)])
	}
}

func BenchmarkThroughput(b *testing.B) {
	b.ResetTimer()
	for range b.N {
		_ = Throughput(104857600, time.Second)
	}
}
