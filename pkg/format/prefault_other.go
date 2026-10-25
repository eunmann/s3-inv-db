//go:build !linux

package format

// Prefault reads an index into the page cache before it serves queries;
// it uses MADV_POPULATE_READ, which only Linux has, so elsewhere it does
// nothing.
func Prefault(string) error { return nil }
