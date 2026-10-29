//go:build linux

package benchutil

import (
	"os"
	"syscall"
)

// allocatedBytes is the space info's file takes on disk, which for a
// preallocated or sparse file differs from its length.
func allocatedBytes(info os.FileInfo) int64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		const statBlockSize = 512

		return st.Blocks * statBlockSize
	}

	return info.Size()
}
