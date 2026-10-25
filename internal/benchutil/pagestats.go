package benchutil

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// PageStats is a snapshot of the process's page-fault and storage-read
// counters. The difference of two snapshots is what the code between
// them cost the memory system: minor faults (a page already in the
// page cache mapped in), major faults (a page read from storage) and
// the bytes the kernel read from storage, readahead included.
type PageStats struct {
	MinorFaults int64
	MajorFaults int64
	// ReadBytes is /proc/self/io read_bytes; zero where unavailable.
	ReadBytes int64
}

// ReadPageStats snapshots the counters for the whole process, so
// measure with nothing else running in it.
func ReadPageStats(tb testing.TB) PageStats {
	tb.Helper()
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		tb.Fatalf("getrusage: %v", err)
	}

	return PageStats{
		MinorFaults: ru.Minflt,
		MajorFaults: ru.Majflt,
		ReadBytes:   procIOReadBytes(),
	}
}

// Sub returns s - prev.
func (s PageStats) Sub(prev PageStats) PageStats {
	return PageStats{
		MinorFaults: s.MinorFaults - prev.MinorFaults,
		MajorFaults: s.MajorFaults - prev.MajorFaults,
		ReadBytes:   s.ReadBytes - prev.ReadBytes,
	}
}

// Report adds the counters to b's results, divided by ops (for example
// the queries the measured code ran).
func (s PageStats) Report(b *testing.B, ops int) {
	b.Helper()
	n := float64(ops)
	b.ReportMetric(float64(s.MinorFaults)/n, "minflt/q")
	b.ReportMetric(float64(s.MajorFaults)/n, "majflt/q")
	b.ReportMetric(float64(s.ReadBytes)/1024/n, "ioKiB/q")
}

// procIOReadBytes reads read_bytes from /proc/self/io, or 0 when the
// file is missing (non-Linux) or unreadable.
func procIOReadBytes() int64 {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0
	}
	for line := range bytes.Lines(data) {
		if v, ok := bytes.CutPrefix(line, []byte("read_bytes:")); ok {
			n, _ := strconv.ParseInt(string(bytes.TrimSpace(v)), 10, 64)

			return n
		}
	}

	return 0
}

// MappedBytes returns, for every file under dir that the process has
// mmapped, the bytes of it currently mapped in (smaps Rss), keyed by
// the path relative to dir. Pages enter a mapping on first touch (plus
// the kernel's fault-around), so on a freshly opened index this is the
// set of pages the queries since the open have touched. Returns nil
// where /proc/self/smaps is unavailable.
func MappedBytes(tb testing.TB, dir string) map[string]int64 {
	tb.Helper()
	f, err := os.Open("/proc/self/smaps")
	if err != nil {
		return nil
	}
	defer f.Close()

	root := filepath.Clean(dir) + string(filepath.Separator)
	out := map[string]int64{}
	var current string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if rss, ok := strings.CutPrefix(line, "Rss:"); ok {
			if current == "" {
				continue
			}
			kb, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rss), "kB")), 10, 64)
			if err == nil {
				out[current] += kb * 1024
			}

			continue
		}
		// A mapping header: "addr perms offset dev inode [path]". Field
		// lines ("Name: value") have a colon in their first field.
		fields := strings.Fields(line)
		if len(fields) >= 5 && !strings.HasSuffix(fields[0], ":") {
			current = ""
			if len(fields) >= 6 && strings.HasPrefix(fields[5], root) {
				current = strings.TrimPrefix(fields[5], root)
			}
		}
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("read smaps: %v", err)
	}

	return out
}
