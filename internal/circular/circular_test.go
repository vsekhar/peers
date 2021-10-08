package circular

import (
	"sync"
	"testing"
	"time"
)

func TestCircular(t *testing.T) {
	c := NewBuffer(3)
	c.Store(1)
	c.Store(2)
	c.LoadOrWait() // --> 1
	c.Store(3)
	c.Store(4)
	c.Store(5) // clobbers 2
	v := c.LoadOrWait().(int)
	if v != 3 {
		t.Fatalf("expected 3, got %d", v)
	}
	c.LoadOrWait() // 4
	c.LoadOrWait() // 5
	if c.nonEmpty != false {
		t.Error("expected nonEmpty == false")
	}
	if c.in != c.out {
		t.Error("expected in and out pointers to be equal")
	}
}

func TestWait(t *testing.T) {
	c := NewBuffer(3)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		v := c.LoadOrWait().(int)
		if v != 1 {
			t.Errorf("expected 1 got %d", v)
		}
	}()

	// Wait for goroutine to block, not technically guaranteed so confirm
	// cond.Wait() was called in coverage.
	time.Sleep(100 * time.Millisecond)

	c.Store(1)
	wg.Wait()
}
