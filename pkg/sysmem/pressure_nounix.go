//go:build !unix

package sysmem

// readProcessFaults has no implementation off unix.
func readProcessFaults(*Pressure) {}
