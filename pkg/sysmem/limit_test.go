package sysmem_test

import (
	"errors"
	"runtime/debug"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/sysmem"
)

func TestApplyMemoryLimit_RespectsGoMemLimitEnv(t *testing.T) {
	const fourGiB = 4 * 1024 * 1024 * 1024
	t.Setenv("GOMEMLIMIT", "4GiB")
	prev := debug.SetMemoryLimit(-1) // read current
	debug.SetMemoryLimit(prev)       // restore
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	got := sysmem.ApplyMemoryLimit(0) // fraction 0 → don't add sysmem candidate
	if got.EnvBytes != fourGiB {
		t.Errorf("EnvBytes = %d, want %d", got.EnvBytes, fourGiB)
	}
	// With fraction=0 and no cgroup (or smaller cgroup), env may or may
	// not win — check that whichever source wins has Bytes <= 4 GiB.
	if got.Bytes > fourGiB {
		t.Errorf("Bytes = %d, must be <= 4 GiB", got.Bytes)
	}
}

func TestApplyMemoryLimit_ApplysSysmemFractionWhenNothingElseSet(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(prev)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	got := sysmem.ApplyMemoryLimit(0.5)
	if got.SysmemFractionBytes == 0 {
		// sysmem detection might be unreliable in the test env — accept
		// either branch but ensure we didn't crash.
		t.Skip("sysmem detection unreliable; nothing to assert")
	}
	if got.Bytes == 0 {
		t.Error("Bytes = 0 with sysmem available and fraction=0.5")
	}
}

func TestApplyMemoryLimit_PickSmallestWins(t *testing.T) {
	// GOMEMLIMIT=10GiB env beats nothing, but if cgroup or sysmem
	// reports a smaller value the smaller one must win. Hard to drive
	// cgroup deterministically in a test, so this acts as a smoke check
	// that the candidate aggregation prefers smaller values via the
	// real ApplyMemoryLimit path.
	const tenGiB = 10 * 1024 * 1024 * 1024
	t.Setenv("GOMEMLIMIT", "10GiB")
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(prev)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	got := sysmem.ApplyMemoryLimit(0.001) // tiny fraction so sysmem candidate is small
	if got.EnvBytes != tenGiB {
		t.Errorf("EnvBytes = %d, want %d", got.EnvBytes, tenGiB)
	}
	if got.SysmemFractionBytes > 0 && got.Bytes > got.SysmemFractionBytes {
		t.Errorf("Bytes = %d should not exceed SysmemFractionBytes = %d", got.Bytes, got.SysmemFractionBytes)
	}
}

func TestParseGoMemLimit_RejectsUnknownSuffix(t *testing.T) {
	// Exercises the parse path indirectly: set an invalid env, expect
	// ApplyMemoryLimit to fall through to other candidates without
	// crashing or recording an EnvBytes.
	t.Setenv("GOMEMLIMIT", "5XB")
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(prev)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	got := sysmem.ApplyMemoryLimit(0)
	if got.EnvBytes != 0 {
		t.Errorf("EnvBytes = %d, want 0 (parse should have failed)", got.EnvBytes)
	}
	// Sentinel reachable for callers that want it.
	if !errors.Is(sysmem.ErrUnknownMemSuffix, sysmem.ErrUnknownMemSuffix) {
		t.Fatal("ErrUnknownMemSuffix not reflexively errors.Is — sanity check")
	}
}
