//go:build !linux

package format

// Prefault reads the files a manifest lists into the page cache before
// the index serves queries; it uses MADV_POPULATE_READ, which only Linux
// has, so elsewhere it does nothing.
func Prefault(string, *Manifest) error { return nil }
