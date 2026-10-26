//go:build !linux

package sysmem

// readLinuxPressure has no implementation off Linux.
func readLinuxPressure(*Pressure) {}
