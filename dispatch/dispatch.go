package dispatch

import (
	"math"

	"github.com/segmentio/fasthash/fnv1a"
)

// Dispatcher selects destinations for a given value i.
//
// For any given Dispatcher state, the destinations chosen for a given value of
// i will be stable.
type Dispatcher interface {
	Dispatch(i uint64) []string
	Len() int
}

type StringDispatcher interface {
	Dispatcher
	DispatchString(s string) []string
}

type stringer struct {
	Dispatcher
}

func (s *stringer) DispatchString(v string) []string {
	// f := fnv.New64a()
	// f.Write([]byte(value))
	// s := f.Sum64()          // 213ns / op
	h := fnv1a.HashString64(v) // 63ns / op
	return s.Dispatch(h)
}

func ByString(d Dispatcher) StringDispatcher {
	return &stringer{Dispatcher: d}
}

// CoalescingDispatcher selects a destination based on the first n bits of each
// key in a key pair.
//
// For any given CoalescingDispatcher state, the destination chosen for a
// given pair of keys at a given bit depth will be stable.
//
// For any given CoalescingDispatcher state, the destination chosen for with
// n equal to zero will be stable, regardless of the values of the keys.
type CoalescingDispatcher interface {
	CoalescingDispatch(key1, key2 string, n int) []string
	MaxBits() int
}

type coalescer struct {
	Dispatcher
}

func (c *coalescer) CoalescingDispatch(key1, key2 string, n int) []string {

	// Derive key with n low-order bits of key1 (1), and n low-order bits
	// of key2 (2) in the highest possible position.
	//
	//   E.g. bits==4 --> key1  == _____1111
	//                    key2  == _____2222
	//                    xor   == _____XXXX
	//                    key   == XXXX0000....
	//
	// Thus key remains determined by key1 and key2, but will coalesce into
	// one of 2^n values. At n==0, key==0 for all values of key1 and key2

	h1, h2 := fnv1a.HashString64(key1), fnv1a.HashString64(key2)
	mask := uint64(math.MaxUint64) >> (64 - n)
	h1 &= mask
	h2 &= mask
	h1 <<= (64 - n)
	h2 <<= (64 - n)
	key := h1 ^ h2
	return c.Dispatch(key)
}

func (c *coalescer) MaxBits() int {
	return int(math.Ceil(math.Log2(float64(c.Len()))))
}

// Coalesce returns a CoalescingDispatcher for a given Dispatcher.
func Coalesce(d Dispatcher) CoalescingDispatcher {
	return &coalescer{Dispatcher: d}
}
