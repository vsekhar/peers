package last

import (
	"sync"
)

type Buffer struct {
	mu    *sync.Mutex
	cond  *sync.Cond
	value interface{}
}

func NewBuffer() *Buffer {
	mu := &sync.Mutex{}
	return &Buffer{
		mu:   mu,
		cond: sync.NewCond(mu),
	}
}

func (b *Buffer) Store(v interface{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.value = v
	b.cond.Signal()
}

func (b *Buffer) LoadOrWait() interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.value == nil {
		b.cond.Wait()
	}
	r := b.value
	b.value = nil
	return r
}
