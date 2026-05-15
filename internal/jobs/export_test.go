package jobs

// NewJobIDForTest exposes newJobID for tests in the jobs_test package.
func NewJobIDForTest() (ID, error) { return newJobID() }
