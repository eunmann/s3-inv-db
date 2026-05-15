package inventory

import (
	"time"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// SetClockForTest injects a clock used to timestamp Refresh()'s
// snapshot. Lets tests assert on the timestamp deterministically.
func (s *DiscoveryService) SetClockForTest(clock func() time.Time) {
	s.bgClock = clock
}

// Test-only exports of internal helpers so external _test packages can
// exercise them without changing production visibility. Defined in
// *_test.go so they are only compiled into the test binary.

// ClassifyForTest exposes the unexported classify helper to external tests.
func ClassifyForTest(objects, bytes CompareNumeric, tierBefore, tierAfter map[string]indexread.TierBreakdown) CompareStatus {
	return classify(objects, bytes, tierBefore, tierAfter)
}

// TierMapsEqualForTest exposes the unexported tierMapsEqual helper to external tests.
func TierMapsEqualForTest(a, b map[string]indexread.TierBreakdown) bool {
	return tierMapsEqual(a, b)
}

// StatusOrderForTest exposes the unexported statusOrder helper to external tests.
func StatusOrderForTest(s CompareStatus) int { return statusOrder(s) }
