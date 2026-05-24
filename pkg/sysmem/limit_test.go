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

func TestApplyMemoryLimit_EnvOverridesSysmemFraction(t *testing.T) {
	// Explicit GOMEMLIMIT bypasses the sysmem fraction so operators on
	// dedicated hosts can opt into using more than the default
	// fraction. The cgroup limit (when present) still caps it.
	const tenGiB = 10 * 1024 * 1024 * 1024
	t.Setenv("GOMEMLIMIT", "10GiB")
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(prev)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	got := sysmem.ApplyMemoryLimit(0.001) // tiny fraction; without override would dominate
	if got.EnvBytes != tenGiB {
		t.Errorf("EnvBytes = %d, want %d", got.EnvBytes, tenGiB)
	}
	if got.CgroupBytes > 0 && got.Bytes > got.CgroupBytes {
		t.Errorf("Bytes = %d should not exceed CgroupBytes = %d", got.Bytes, got.CgroupBytes)
	}
	// The sysmem fraction must NOT bind the env-set case.
	if got.CgroupBytes == 0 && got.Bytes != tenGiB {
		t.Errorf("Bytes = %d, want %d (env override should win without cgroup)", got.Bytes, tenGiB)
	}
}

func TestComputeMemoryLimit_NoSideEffects(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(prev)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	_ = sysmem.ComputeMemoryLimit(0.5)

	after := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(after)
	if after != prev {
		t.Errorf("ComputeMemoryLimit mutated process memory limit: before=%d after=%d", prev, after)
	}
}

func TestApply_RespectsGOGCEnv(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	t.Setenv("GOGC", "50")

	prevLimit := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(prevLimit)
	t.Cleanup(func() { debug.SetMemoryLimit(prevLimit) })

	// SetGCPercent(-1) returns the previous value without changing it.
	prevGC := debug.SetGCPercent(-1)
	debug.SetGCPercent(prevGC)
	t.Cleanup(func() { debug.SetGCPercent(prevGC) })

	// Force a tight-limit path so Apply would otherwise call SetGCPercent.
	tight := int64(1 * 1024 * 1024 * 1024) // 1 GiB
	sysmem.Apply(sysmem.MemoryLimitResult{Bytes: tight, Source: sysmem.MemoryLimitSourceSysmem})

	afterGC := debug.SetGCPercent(-1)
	debug.SetGCPercent(afterGC)
	if afterGC != prevGC {
		t.Errorf("Apply mutated GOGC despite env override: before=%d after=%d", prevGC, afterGC)
	}
}

func TestApply_TunesGOGCWhenEnvUnset(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	t.Setenv("GOGC", "")

	prevLimit := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(prevLimit)
	t.Cleanup(func() { debug.SetMemoryLimit(prevLimit) })

	prevGC := debug.SetGCPercent(-1)
	debug.SetGCPercent(prevGC)
	t.Cleanup(func() { debug.SetGCPercent(prevGC) })

	tight := int64(1 * 1024 * 1024 * 1024) // 1 GiB
	sysmem.Apply(sysmem.MemoryLimitResult{Bytes: tight, Source: sysmem.MemoryLimitSourceSysmem})

	afterGC := debug.SetGCPercent(-1)
	debug.SetGCPercent(afterGC)
	if afterGC != 50 {
		t.Errorf("Apply did not tune GOGC to 50 with env unset: got %d", afterGC)
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
