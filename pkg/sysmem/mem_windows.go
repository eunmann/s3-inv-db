//go:build windows

package sysmem

import (
	"syscall"
	"unsafe"
)

// memoryStatusEx matches the Windows MEMORYSTATUSEX structure.
// https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/ns-sysinfoapi-memorystatusex
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	procGlobalMemoryStatusEx = kernel32.NewProc("GlobalMemoryStatusEx")
)

// totalSystemMemory uses GlobalMemoryStatusEx for total physical RAM.
func totalSystemMemory() (uint64, bool) {
	var memStatus memoryStatusEx
	memStatus.Length = uint32(unsafe.Sizeof(memStatus))

	ret, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memStatus)))
	if ret == 0 {
		return 0, false
	}

	return memStatus.TotalPhys, true
}

// cgroupMemoryMax has no Windows analogue; ApplyMemoryLimit skips it.
func cgroupMemoryMax() (uint64, bool) { return 0, false }
