package circular

import (
	"sync"
)

type Buffer struct {
	mu      *sync.Mutex
	cond    *sync.Cond
	values  []interface{}
	in, out int
}

func NewBuffer(max int) *Buffer {
	mu := &sync.Mutex{}
	return &Buffer{
		mu:     mu,
		cond:   sync.NewCond(mu),
		values: make([]interface{}, max),
	}
}

func (b *Buffer) Store(v interface{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.values[b.in] = v
	b.in = (b.in + 1) % len(b.values)
	if b.out == b.in {
		b.out = (b.out + 1) % len(b.values)
	}
	b.cond.Signal()
}

func (b *Buffer) LoadOrWait() interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.in == b.out {
		b.cond.Wait()
	}
	r := b.values[b.out]
	b.out = (b.out + 1) % len(b.values)
	return r
}
