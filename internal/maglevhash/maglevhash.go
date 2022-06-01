// Package maglevhash implements the hash function from Maglev, Google's fast
// load balancer.
//
//   https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/44824.pdf
package maglevhash

import (
	"crypto/sha1"
	"fmt"
	"math"
	"math/big"
	"sort"

	"github.com/segmentio/fasthash/fnv1a"
	"github.com/vsekhar/peers/dispatch"
)

const (
	memberTableFactor = 100
	coarsePrimeSafety = 32
	finePrimeSafety   = 1000
)

func nextPrime(n int) int {
	for ; ; n++ {
		b := big.NewInt(int64(n)) // at most 2^63

		// "ProbablyPrime is 100% accurate for inputs less than 2⁶⁴."
		// -https://pkg.go.dev/math/big#Int.ProbablyPrime
		if b.ProbablyPrime(0) {
			return n
		}
	}
}

func m(n int) int {
	i := 1
	for i < n*memberTableFactor {
		i <<= 1
	}
	return nextPrime(i)
}

var _ dispatch.Dispatcher = (*MaglevHasher)(nil)

type MaglevHasher struct {
	members int
	table   [][]string // table[entryNo][replicaNo] --> memberName
	binSize uint64
}

// New creates a new MaglevHasher consisting of members.
func New(members []string, replicas int) (*MaglevHasher, error) {
	if replicas < 1 {
		return nil, fmt.Errorf("replicas %d, must be greater than 0", replicas)
	}
	N := len(members)
	M := m(N)
	r := &MaglevHasher{
		members: len(members),
		binSize: math.MaxUint64 / uint64(M),
		table:   make([][]string, M),
	}
	for i := range r.table {
		r.table[i] = make([]string, 0, replicas) // preallocate
	}
	if len(members) == 0 {
		return r, nil
	}

	sorted := append([]string(nil), members...)
	sort.Strings(sorted)
	for i := 0; i < len(sorted)-1; i++ {
		if sorted[i] == sorted[i+1] {
			panic(fmt.Sprintf("maglevhash: duplicate member name '%s'", sorted[i]))
		}
	}

	stats := make([]struct{ offset, skip, next int }, N)
	for i, m := range sorted {
		v1 := fnv1a.HashString32(m)
		h2 := sha1.Sum([]byte(m))
		v2 := fnv1a.HashBytes32(h2[:])
		stats[i].offset = int(v1) % M
		stats[i].skip = (int(v2) % (M - 1)) + 1
	}
	done := 0
	for done < len(r.table) {
		for i, name := range sorted {
			// next preference for member i
			p := (stats[i].offset + stats[i].next*stats[i].skip) % M
			stats[i].next++
			if len(r.table[p]) < replicas {
				r.table[p] = append(r.table[p], name)
				if len(r.table[p]) == replicas {
					done++
				}
			}
		}
	}
	return r, nil
}

// Dispatch gets the members corresponding to i. Dispatch evenly distributes all
// possible unsigned integers i across members.
func (m *MaglevHasher) Dispatch(i uint64) []string {
	// TODO: should this be mod? It might break coalescing
	// Expose an API that allows that choice to be made at a higher level?

	return m.table[i/m.binSize]
}

func (m *MaglevHasher) Len() int { return m.members }
