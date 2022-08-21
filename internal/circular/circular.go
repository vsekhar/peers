package circular

import (
	"container/ring"
	"sync"
)

type Buffer[T any] struct {
	mu       *sync.Mutex
	cond     *sync.Cond
	in       *ring.Ring
	out      *ring.Ring
	nonEmpty bool
}

func NewBuffer[T any](n int) *Buffer[T] {
	mu := &sync.Mutex{}
	r := ring.New(n)
	return &Buffer[T]{
		mu:   mu,
		cond: sync.NewCond(mu),
		in:   r,
		out:  r,
	}
}

func (b *Buffer[T]) Store(v T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.in.Value = v
	if b.in == b.out && b.nonEmpty {
		b.out = b.out.Next()
	}
	b.in = b.in.Next()
	b.nonEmpty = true
	b.cond.Signal()
}

func (b *Buffer[T]) LoadOrWait() T {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.in == b.out && !b.nonEmpty {
		b.cond.Wait()
	}
	r := b.out.Value
	b.out = b.out.Next()
	if b.in == b.out {
		b.nonEmpty = false
	}
	return r.(T)
}
