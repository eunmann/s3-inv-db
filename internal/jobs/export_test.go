package jobs

import "time"

// NewJobIDForTest exposes newJobID for tests in the jobs_test package.
func NewJobIDForTest() (ID, error) { return newJobID() }

// UpsertStore is the store surface the Scheduler writes through.
type UpsertStore = jobStore

// NewSchedulerForTest builds a Scheduler over any store, writing running
// progress at most once per every.
func NewSchedulerForTest(store UpsertStore, bus *Bus, every time.Duration) *Scheduler {
	s := NewScheduler(nil, bus)
	s.store = store
	s.progressPersistEvery = every

	return s
}
