package autoload

import "context"

// Tick exposes the unexported tick loop for tests in autoload_test.
func (a *AutoLoader) Tick(ctx context.Context) { a.tick(ctx) }

// BackoffDelay exposes the unexported backoff helper for tests.
var BackoffDelay = backoffDelay
