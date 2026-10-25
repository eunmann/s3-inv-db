//go:build !linux

package benchutil

import "testing"

// HWCounters needs perf_event_open, which only Linux provides; here
// every method is a no-op.
type HWCounters struct{}

func NewHWCounters(testing.TB) *HWCounters { return nil }

func (*HWCounters) Start()                 {}
func (*HWCounters) Stop()                  {}
func (*HWCounters) Report(*testing.B, int) {}
func (*HWCounters) Close()                 {}
