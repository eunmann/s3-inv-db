//go:build !linux

package benchutil

import "os"

// allocatedBytes is info's file length: block counts are not portable.
func allocatedBytes(info os.FileInfo) int64 {
	return info.Size()
}
