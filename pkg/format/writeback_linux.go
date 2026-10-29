package format

import (
	"os"

	"golang.org/x/sys/unix"
)

// startWriteback asks the kernel to start writing f's dirty pages to
// disk as f is finished, so the sync before publishing finds little
// left to write. It runs on a duplicate descriptor in a goroutine of
// its own: on a throttled device, submitting the writes can block about
// as long as the sync itself. It is only a hint, so its errors are
// dropped: a failed write is reported by that sync.
func startWriteback(f *os.File) {
	fd, err := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return
	}
	go func() {
		_ = unix.SyncFileRange(fd, 0, 0, unix.SYNC_FILE_RANGE_WRITE)
		_ = unix.Close(fd)
	}()
}
