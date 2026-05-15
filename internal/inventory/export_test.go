package inventory

import "github.com/eunmann/s3-inv-db/pkg/indexread"

// Test-only exports of internal helpers so external _test packages can
// exercise them without changing production visibility. Defined in
// *_test.go so they are only compiled into the test binary.

// ClassifyForTest exposes classify(...) to inventory_test.
func ClassifyForTest(objects, bytes CompareNumeric, tierBefore, tierAfter map[string]indexread.TierBreakdown) CompareStatus {
	return classify(objects, bytes, tierBefore, tierAfter)
}

// TierMapsEqualForTest exposes tierMapsEqual(...) to inventory_test.
func TierMapsEqualForTest(a, b map[string]indexread.TierBreakdown) bool {
	return tierMapsEqual(a, b)
}

// StatusOrderForTest exposes statusOrder(...) to inventory_test.
func StatusOrderForTest(s CompareStatus) int { return statusOrder(s) }
