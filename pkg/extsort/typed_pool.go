package extsort

import "sync"

// typedPool wraps sync.Pool with a single generic type-assertion point
// so call sites Get/Put concrete *T instead of interface{}.
type typedPool[T any] struct {
	pool sync.Pool
}

func newTypedPool[T any](newFn func() *T) *typedPool[T] {
	return &typedPool[T]{
		pool: sync.Pool{
			New: func() any { return newFn() },
		},
	}
}

// Get returns a *T from the pool. The cast cannot fail because the
// pool's New constructor returns *T and Put only accepts *T.
func (p *typedPool[T]) Get() *T {
	//nolint:forcetypeassert // pool.New constrains the element type to *T
	return p.pool.Get().(*T)
}

func (p *typedPool[T]) Put(v *T) {
	p.pool.Put(v)
}
