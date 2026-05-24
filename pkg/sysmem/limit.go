package sysmem

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// DefaultMemoryLimitFraction is the fraction of detected system RAM
// applied as GOMEMLIMIT when no tighter cgroup or env limit overrides
// it. 0.6 leaves room for off-heap allocations (mmap, CGO, OS page
// cache) the Go runtime does not account for.
const DefaultMemoryLimitFraction = 0.6

// MemoryLimitSource explains how the resolved limit was picked.
type MemoryLimitSource string

// Known sources for MemoryLimitResult.Source.
const (
	MemoryLimitSourceEnv     MemoryLimitSource = "GOMEMLIMIT"
	MemoryLimitSourceCgroup  MemoryLimitSource = "cgroup-memory-max"
	MemoryLimitSourceSysmem  MemoryLimitSource = "sysmem-fraction"
	MemoryLimitSourceDefault MemoryLimitSource = "default"
)

// MemoryLimitResult describes the resolved process memory limit.
type MemoryLimitResult struct {
	Source              MemoryLimitSource
	Bytes               int64
	EnvBytes            int64
	CgroupBytes         int64
	SysmemFractionBytes int64
}

// ErrUnknownMemSuffix is returned by parseGoMemLimit when the suffix on
// a GOMEMLIMIT value isn't one of the units Go's runtime documents.
var ErrUnknownMemSuffix = errors.New("unknown GOMEMLIMIT suffix")

// ComputeMemoryLimit resolves the soft process memory limit without
// any debug.* side effects. The chosen value is:
//   - min(GOMEMLIMIT, cgroup) when GOMEMLIMIT is explicitly set — the
//     operator's value wins over the sysmem fraction, capped only by
//     the container limit so a too-permissive env can't bust the cgroup.
//   - min(cgroup, fraction × Total RAM) otherwise.
//
// Pass DefaultMemoryLimitFraction (0.6) unless you have a reason to deviate.
func ComputeMemoryLimit(fraction float64) MemoryLimitResult {
	out := MemoryLimitResult{Source: MemoryLimitSourceDefault}

	envExplicit := false
	if env := os.Getenv("GOMEMLIMIT"); env != "" {
		envExplicit = true
		if n, err := parseGoMemLimit(env); err == nil && n > 0 {
			out.EnvBytes = n
		}
	}
	if n, ok := cgroupMemoryMax(); ok {
		out.CgroupBytes = int64(n)
	}
	if r := Total(); r.Reliable && fraction > 0 {
		out.SysmemFractionBytes = int64(float64(r.TotalBytes) * fraction)
	}

	if envExplicit && out.EnvBytes > 0 {
		out.Bytes, out.Source = pickSmallest(MemoryLimitResult{
			EnvBytes:    out.EnvBytes,
			CgroupBytes: out.CgroupBytes,
		})
	} else {
		out.Bytes, out.Source = pickSmallest(out)
	}

	return out
}

// Apply installs the resolved memory limit via debug.SetMemoryLimit
// and, when the limit is tight, lowers GOGC to keep headroom.
//
// GOGC env handling: if GOGC is set in the environment to a value the
// runtime accepts (a decimal integer or "off"), Apply does NOT call
// debug.SetGCPercent — the operator's choice wins. Otherwise Apply
// may tune GOGC down from the default 100 to reduce thrash near
// GOMEMLIMIT at small budgets.
func Apply(result MemoryLimitResult) {
	if result.Bytes <= 0 {
		return
	}
	debug.SetMemoryLimit(result.Bytes)
	const (
		tightLimitBytes = 4 * 1024 * 1024 * 1024 // 4 GiB
		tightGCPercent  = 50
	)
	if result.Bytes > tightLimitBytes {
		return
	}
	if gogcEnvSet() {
		return
	}
	debug.SetGCPercent(tightGCPercent)
}

// ApplyMemoryLimit composes ComputeMemoryLimit and Apply for callers
// that want both in one step.
func ApplyMemoryLimit(fraction float64) MemoryLimitResult {
	result := ComputeMemoryLimit(fraction)
	Apply(result)

	return result
}

// gogcEnvSet reports whether GOGC is set to a value the Go runtime
// would honor (decimal integer or "off"). Empty/unparseable values
// are ignored so we don't yield to garbage env.
func gogcEnvSet() bool {
	v := strings.TrimSpace(os.Getenv("GOGC"))
	if v == "" {
		return false
	}
	if v == "off" {
		return true
	}
	_, err := strconv.Atoi(v)

	return err == nil
}

func pickSmallest(r MemoryLimitResult) (int64, MemoryLimitSource) {
	candidates := []struct {
		source MemoryLimitSource
		bytes  int64
	}{
		{source: MemoryLimitSourceEnv, bytes: r.EnvBytes},
		{source: MemoryLimitSourceCgroup, bytes: r.CgroupBytes},
		{source: MemoryLimitSourceSysmem, bytes: r.SysmemFractionBytes},
	}
	var (
		bestBytes  int64
		bestSource = MemoryLimitSourceDefault
	)
	for _, c := range candidates {
		if c.bytes <= 0 {
			continue
		}
		if bestBytes == 0 || c.bytes < bestBytes {
			bestBytes = c.bytes
			bestSource = c.source
		}
	}

	return bestBytes, bestSource
}

// parseGoMemLimit accepts the same syntax `debug.SetMemoryLimit` and
// the GOMEMLIMIT env var documented by Go: a decimal number followed
// by one of B, KiB, MiB, GiB, TiB. We don't accept "off" — callers
// pass an explicit zero/negative fraction to disable.
//

func parseGoMemLimit(s string) (int64, error) {
	const (
		kib = 1024
		mib = kib * 1024
		gib = mib * 1024
		tib = gib * 1024
	)
	s = strings.TrimSpace(s)
	if s == "" || s == "off" {
		return 0, nil
	}
	numEnd := len(s)
	for i, c := range s {
		if (c < '0' || c > '9') && c != '.' {
			numEnd = i

			break
		}
	}
	numStr, suffix := s[:numEnd], s[numEnd:]
	num, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse GOMEMLIMIT %q: %w", s, err)
	}
	var mult int64
	switch suffix {
	case "", "B":
		mult = 1
	case "KiB":
		mult = kib
	case "MiB":
		mult = mib
	case "GiB":
		mult = gib
	case "TiB":
		mult = tib
	default:
		return 0, fmt.Errorf("%w %q", ErrUnknownMemSuffix, suffix)
	}

	return num * mult, nil
}
