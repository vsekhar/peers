package singlego

import "context"

type Trigger chan struct{}

func New(ctx context.Context, f func()) Trigger {
	r := make(chan struct{}, 1) // must be buffered
	go func() {
		for {
			select {
			case <-r:
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
