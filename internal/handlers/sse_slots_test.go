package handlers_test

import (
	"fmt"
	"sync"
	"testing"
)

func TestSSESlots_PruneIPWhenCountReachesZero(t *testing.T) {
	h := newTestHandlers(t)
	for i := range 50 {
		ip := fmt.Sprintf("10.0.0.%d", i)
		h.AcquireSSESlotForTest(ip)
		h.AcquireSSESlotForTest(ip)
		h.ReleaseSSESlotForTest(ip)
		if got := h.SSETrackedIPsForTest(); got != 1 {
			t.Fatalf("%s: after one release of two, tracked = %d, want 1", ip, got)
		}
		h.ReleaseSSESlotForTest(ip)
	}
	if got := h.SSETrackedIPsForTest(); got != 0 {
		t.Errorf("tracked IPs = %d after every slot released, want 0", got)
	}
}

func TestSSESlots_ConcurrentChurnKeepsCountsExact(t *testing.T) {
	h := newTestHandlers(t)
	const held = "192.0.2.1"
	h.AcquireSSESlotForTest(held)

	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			other := fmt.Sprintf("198.51.100.%d", g)
			for range 500 {
				h.AcquireSSESlotForTest(held)
				h.ReleaseSSESlotForTest(held)
				h.AcquireSSESlotForTest(other)
				h.ReleaseSSESlotForTest(other)
			}
		})
	}
	wg.Wait()

	if got := h.AcquireSSESlotForTest(held); got != 2 {
		t.Errorf("held IP count after churn = %d, want 2 (the held slot plus this one)", got)
	}
	h.ReleaseSSESlotForTest(held)
	h.ReleaseSSESlotForTest(held)
	if got := h.SSETrackedIPsForTest(); got != 0 {
		t.Errorf("tracked IPs = %d after churn, want 0", got)
	}
}
