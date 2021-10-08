package circular

import (
	"container/ring"
	"sync"
)

type Buffer struct {
	mu       *sync.Mutex
	cond     *sync.Cond
	in       *ring.Ring
	out      *ring.Ring
	nonEmpty bool
}

func NewBuffer(n int) *Buffer {
	mu := &sync.Mutex{}
	r := ring.New(n)
	return &Buffer{
		mu:   mu,
		cond: sync.NewCond(mu),
		in:   r,
		out:  r,
	}
}

func (b *Buffer) Store(v interface{}) {
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

func (b *Buffer) LoadOrWait() interface{} {
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
	return r
}
