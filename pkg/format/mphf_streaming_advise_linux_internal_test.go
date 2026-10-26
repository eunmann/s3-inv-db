//go:build linux

package format

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"
	"unsafe"
)

// TestStreamingMPHFBuilder_ArraysKeepReadahead pins that the build maps
// its scratch arrays without MADV_RANDOM: every reader scans them in
// position order, and an array that misses the page cache needs
// readahead to be read in large chunks instead of a fault per page.
func TestStreamingMPHFBuilder_ArraysKeepReadahead(t *testing.T) {
	b, err := NewStreamingMPHFBuilder(t.TempDir())
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder: %v", err)
	}
	defer b.Close()
	for i := range 4096 {
		if err := b.Add(fmt.Sprintf("p%d/", i)); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := b.freezeArrays(); err != nil {
		t.Fatalf("freezeArrays: %v", err)
	}

	for name, a := range map[string]*u64DiskArray{"hashes": b.hashes, "fingerprints": b.fingerprints} {
		flags := mappingVMFlags(t, uintptr(unsafe.Pointer(&a.mmap[0])))
		if strings.Contains(" "+flags+" ", " rr ") {
			t.Errorf("%s mapping is advised MADV_RANDOM (VmFlags %q); its readers scan it in order", name, flags)
		}
	}
}

// mappingVMFlags returns the VmFlags line of the /proc/self/smaps entry
// whose range holds addr.
func mappingVMFlags(t *testing.T, addr uintptr) string {
	t.Helper()
	f, err := os.Open("/proc/self/smaps")
	if err != nil {
		t.Skipf("no /proc/self/smaps: %v", err)
	}
	defer f.Close()

	inRange := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		var lo, hi uintptr
		if n, _ := fmt.Sscanf(line, "%x-%x", &lo, &hi); n == 2 {
			inRange = addr >= lo && addr < hi

			continue
		}
		if flags, ok := strings.CutPrefix(line, "VmFlags:"); ok && inRange {
			return strings.TrimSpace(flags)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read smaps: %v", err)
	}
	t.Fatalf("no smaps entry holds %#x", addr)

	return ""
}
