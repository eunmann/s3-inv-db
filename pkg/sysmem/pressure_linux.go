//go:build linux

package sysmem

import (
	"bufio"
	"bytes"
	"os"
	"strconv"
	"time"
)

// readLinuxPressure reads the process's storage I/O from /proc/self/io,
// the system's file refaults from /proc/vmstat and its full I/O stall
// time from /proc/pressure/io.
func readLinuxPressure(p *Pressure) {
	p.ReadBytes = procField("/proc/self/io", "read_bytes:")
	p.WrittenBytes = procField("/proc/self/io", "write_bytes:")
	p.FileRefaults = procField("/proc/vmstat", "workingset_refault_file")
	p.IOStallFull = time.Duration(psiFullTotal("/proc/pressure/io")) * time.Microsecond
}

// procField returns the number after name on the line that starts
// with it ("name value" or "name: value"), or 0.
func procField(path, name string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	for line := range bytes.Lines(data) {
		rest, ok := bytes.CutPrefix(line, []byte(name))
		if !ok || len(rest) == 0 || (rest[0] != ' ' && rest[0] != '\t') {
			continue
		}
		v, err := strconv.ParseInt(string(bytes.TrimSpace(rest)), 10, 64)
		if err != nil {
			return 0
		}

		return v
	}

	return 0
}

// psiFullTotal returns the total= field of the "full" line of a
// pressure stall file, in microseconds, or 0.
func psiFullTotal(path string) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		rest, ok := bytes.CutPrefix(sc.Bytes(), []byte("full "))
		if !ok {
			continue
		}
		for field := range bytes.FieldsSeq(rest) {
			if v, ok := bytes.CutPrefix(field, []byte("total=")); ok {
				n, err := strconv.ParseInt(string(v), 10, 64)
				if err != nil {
					return 0
				}

				return n
			}
		}
	}

	return 0
}
