package sysmem

import "time"

// Pressure is a snapshot of the counters that show a process waiting on
// storage, as when its memory-mapped files do not fit in the page
// cache: its own major page faults and storage I/O, and the system's
// file refaults and I/O stall time. A counter the platform does not
// expose stays zero.
type Pressure struct {
	// MajorFaults are the process's page faults that read from storage.
	MajorFaults int64
	// ReadBytes and WrittenBytes are the process's bytes read from and
	// written to storage (Linux).
	ReadBytes    int64
	WrittenBytes int64
	// FileRefaults are file pages the system re-read soon after
	// evicting them: page-cache thrash (Linux).
	FileRefaults int64
	// IOStallFull is how long every non-idle task on the system was
	// stalled on I/O at once (Linux pressure stall information).
	IOStallFull time.Duration
}

// ReadPressure snapshots the counters. Unreadable counters stay zero:
// they are diagnostics, never a reason to fail.
func ReadPressure() Pressure {
	var p Pressure
	readProcessFaults(&p)
	readLinuxPressure(&p)

	return p
}

// Since returns the counters' growth from earlier to p.
func (p Pressure) Since(earlier Pressure) Pressure {
	return Pressure{
		MajorFaults:  p.MajorFaults - earlier.MajorFaults,
		ReadBytes:    p.ReadBytes - earlier.ReadBytes,
		WrittenBytes: p.WrittenBytes - earlier.WrittenBytes,
		FileRefaults: p.FileRefaults - earlier.FileRefaults,
		IOStallFull:  p.IOStallFull - earlier.IOStallFull,
	}
}
