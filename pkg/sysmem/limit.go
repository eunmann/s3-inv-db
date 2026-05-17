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
	// Bytes is the soft memory limit installed via debug.SetMemoryLimit.
	// Zero means no limit was applied (detection failed completely).
	Bytes int64

	// Source reports which input drove the chosen limit when multiple
	// candidates exist; the smallest wins so this is the binding one.
	Source MemoryLimitSource

	// EnvBytes, CgroupBytes, and SysmemFractionBytes report each
	// candidate independently so callers can log the full picture.
	// Zero means the candidate wasn't available.
	EnvBytes            int64
	CgroupBytes         int64
	SysmemFractionBytes int64
}

// ErrUnknownMemSuffix is returned by parseGoMemLimit when the suffix on
// a GOMEMLIMIT value isn't one of the units Go's runtime documents.
var ErrUnknownMemSuffix = errors.New("unknown GOMEMLIMIT suffix")

// ApplyMemoryLimit computes a soft process memory limit and installs
// it via runtime/debug.SetMemoryLimit. The chosen value is the minimum
// of three candidates:
//   - GOMEMLIMIT env var, if set
//   - cgroup v2 memory.max, if readable
//   - fraction × Total() system RAM
//
// Taking the minimum (rather than respecting env unconditionally) means
// a too-permissive GOMEMLIMIT can't override a tighter container cap.
// Pass DefaultMemoryLimitFraction (0.6) unless you have a reason to deviate.
func ApplyMemoryLimit(fraction float64) MemoryLimitResult {
	out := MemoryLimitResult{Source: MemoryLimitSourceDefault}

	if env := os.Getenv("GOMEMLIMIT"); env != "" {
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

	out.Bytes, out.Source = pickSmallest(out)
	if out.Bytes > 0 {
		debug.SetMemoryLimit(out.Bytes)
		// Tune GOGC down from the default 100 when the soft limit is
		// tight. At GOGC=100 the runtime targets a 2× live-heap peak,
		// which thrashes near GOMEMLIMIT. Sliding to 50 at small
		// budgets trades a bit of CPU for steadier headroom; large
		// budgets keep the default. Operators can still override via
		// the GOGC env (which we don't read here) if they prefer.
		const (
			tightLimitBytes = 4 * 1024 * 1024 * 1024 // 4 GiB
			tightGCPercent  = 50
		)
		if out.Bytes <= tightLimitBytes {
			debug.SetGCPercent(tightGCPercent)
		}
	}

	return out
}

func pickSmallest(r MemoryLimitResult) (int64, MemoryLimitSource) {
	candidates := []struct {
		bytes  int64
		source MemoryLimitSource
	}{
		{r.EnvBytes, MemoryLimitSourceEnv},
		{r.CgroupBytes, MemoryLimitSourceCgroup},
		{r.SysmemFractionBytes, MemoryLimitSourceSysmem},
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
