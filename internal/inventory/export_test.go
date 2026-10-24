package inventory

import (
	"time"
)

// SetClockForTest injects a clock used to timestamp Refresh()'s
// snapshot. Lets tests assert on the timestamp deterministically.
func (d *Discovery) SetClockForTest(clock func() time.Time) {
	d.bgClock = clock
}

// Test-only exports of internal helpers so external _test packages can
// exercise them without changing production visibility. Defined in
// *_test.go so they are only compiled into the test binary.

// SetMirrorForTest swaps the persistence sink for one the test controls
// (e.g. a store that blocks, to prove readers don't wait on it).
func (c *Catalog) SetMirrorForTest(m mirrorStore) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store = m
}
