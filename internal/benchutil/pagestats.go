package benchutil

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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

	return ReadProcStats(tb).PageStats
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

// procIO holds the /proc/self/io byte counters, all from one read.
type procIO struct {
	read, write, cancelledWrite int64
}

// readProcIO reads /proc/self/io once, or returns zeros when the file is
// missing (non-Linux) or unreadable.
func readProcIO() procIO {
	var c procIO
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return c
	}
	for line := range bytes.Lines(data) {
		name, v, _ := bytes.Cut(line, []byte(" "))
		n, _ := strconv.ParseInt(string(bytes.TrimSpace(v)), 10, 64)
		switch string(name) {
		case "read_bytes:":
			c.read = n
		case "write_bytes:":
			c.write = n
		case "cancelled_write_bytes:":
			c.cancelledWrite = n
		}
	}

	return c
}

// ProcStats adds the process's CPU time and storage writes to its page
// stats. Every thread counts, so a parallel build's whole cost shows,
// where its wall time hides work spread over idle cores.
type ProcStats struct {
	PageStats

	User, Sys time.Duration
	// WriteBytes is /proc/self/io write_bytes less cancelled_write_bytes:
	// what was sent to storage, not counting pages of files deleted or
	// truncated before writeback (scratch the build removed in time).
	// Zero where unavailable.
	WriteBytes int64
}

// ReadProcStats snapshots the counters for the whole process, so
// measure with nothing else running in it. One getrusage and one
// /proc/self/io read make the snapshot, so its counters agree in time.
func ReadProcStats(tb testing.TB) ProcStats {
	tb.Helper()
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		tb.Fatalf("getrusage: %v", err)
	}
	pio := readProcIO()

	return ProcStats{
		MinorFaults: ru.Minflt,
		MajorFaults: ru.Majflt,
		ReadBytes:   pio.read,
		User:        time.Duration(ru.Utime.Nano()),
		Sys:         time.Duration(ru.Stime.Nano()),
		WriteBytes:  pio.write - pio.cancelledWrite,
	}
}

// Sub returns s - prev.
func (s ProcStats) Sub(prev ProcStats) ProcStats {
	return ProcStats{
		PageStats:  s.PageStats.Sub(prev.PageStats),
		User:       s.User - prev.User,
		Sys:        s.Sys - prev.Sys,
		WriteBytes: s.WriteBytes - prev.WriteBytes,
	}
}

// Add returns s + d, to total several measured spans.
func (s ProcStats) Add(d ProcStats) ProcStats {
	return ProcStats{
		MinorFaults: s.MinorFaults + d.MinorFaults,
		MajorFaults: s.MajorFaults + d.MajorFaults,
		ReadBytes:   s.ReadBytes + d.ReadBytes,
		User:        s.User + d.User,
		Sys:         s.Sys + d.Sys,
		WriteBytes:  s.WriteBytes + d.WriteBytes,
	}
}

// Report adds the counters to b's results per op: CPU seconds (user and
// system), system seconds, cores (CPU time over wall, the spans' mean
// parallelism), page faults and storage bytes read and written.
func (s ProcStats) Report(b *testing.B, ops int, wall time.Duration) {
	b.Helper()
	n := float64(ops)
	cpu := s.User + s.Sys
	b.ReportMetric(cpu.Seconds()/n, "cpu_s")
	b.ReportMetric(s.Sys.Seconds()/n, "sys_s")
	if wall > 0 {
		b.ReportMetric(cpu.Seconds()/wall.Seconds(), "cores")
	}
	b.ReportMetric(float64(s.MinorFaults)/n, "minflt")
	b.ReportMetric(float64(s.MajorFaults)/n, "majflt")
	b.ReportMetric(float64(s.ReadBytes)/n, "read_B")
	b.ReportMetric(float64(s.WriteBytes)/n, "write_B")
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
