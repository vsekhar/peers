package singlego

// Package singlego is similar to golang.org/x/sync/singleflight except we skip
// the work to obtain and pass on return values. This is more suited to local
// function calls (hence "go" in singlego) where the closure will handle routing
// results and/or logging errors, as opposed to remote API calls which is the
// target of singleflight.

import "context"

type Trigger chan struct{}

func New(ctx context.Context, f func()) Trigger {
	r := make(chan struct{}, 1) // must be buffered
	go func() {
		for {
			select {
			case _, ok := <-r:
				if !ok {
					return
				}
				if f != nil {
					f()
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return r
}

func (t Trigger) Notify() {
	select {
	case t <- struct{}{}:
	default:
	}
}

func (t Trigger) Close() error {
	close(t)
	return nil
}
