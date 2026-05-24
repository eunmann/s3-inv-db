package handlers_test

import (
	"context"
	"errors"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
)

// TestComputeCompareLevel_RespectsContextCancellation guards against
// the previous bug where computeCompareLevel ignored ctx entirely
// (signature was `_ context.Context`). A pre-cancelled context must
// short-circuit before any inventory lookup or index walk.
func TestComputeCompareLevel_RespectsContextCancellation(t *testing.T) {
	h := newTestHandlers(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := h.ComputeCompareLevelForTest(ctx, handlers.CompareViewOptionsForTest{
		From:   "src/inv/run-a",
		To:     "src/inv/run-b",
		Prefix: "",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled wrapped error", err)
	}
}
