//go:build !linux

package format

import "os"

// startWriteback is a no-op where there is no sync_file_range: the
// sync before publishing writes the file.
func startWriteback(*os.File) {}
