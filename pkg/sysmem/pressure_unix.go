//go:build unix

package sysmem

import "golang.org/x/sys/unix"

// readProcessFaults reads the process's major faults from getrusage.
func readProcessFaults(p *Pressure) {
	var ru unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &ru) == nil {
		p.MajorFaults = ru.Majflt
	}
}
