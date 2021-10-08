package backoff

import (
	"context"
	"time"
)

// Probe arranges for prober to be called periodically using an exponential
// backoff interval between calls each time prober returns true. If prober
// returns false, Probe resets the interval to it's starting value.
//
// Probe will wait at least 1s and at most 32s between calls to prober. Probe
// will backoff approximately exponentially between calls. The time taken for
// prober to complete is not considered when calculating the interval. As a
// result, calls to prober may be >32s apart.
//
// Cancelling the provided context will terminate calls to prober. Any call to
// prober that is inflight will not be interrupted.
//
// Probe returns immediately and calls to prober occur in a separate goroutine.
// For any given call to Probe, only one invocation of prober will run at a
// time.
func Probe(ctx context.Context, prober func() bool) {
	const maxShiftReg = 1 << 5 // 32 seconds

	go func() {
		shiftReg := 1 * time.Second
		if prober() {
			shiftReg <<= 1
		} else {
			shiftReg = 1
		}
		if shiftReg > maxShiftReg {
			shiftReg = maxShiftReg
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(shiftReg):
		}
	}()
}
