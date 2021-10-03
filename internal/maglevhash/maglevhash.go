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
	coarsePrimeSafety = 5
	finePrimeSafety   = 1000
)

func nextPrime(n int) int {
	i := big.NewInt(int64(n))
	one := big.NewInt(1)
	for {
		i.Add(i, one)
		if i.ProbablyPrime(coarsePrimeSafety) {
			if i.ProbablyPrime(finePrimeSafety) {
				return int(i.Int64())
			}
		}
	}
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

	sorted := append([]string(nil), members...)
	sort.Strings(sorted)
	for i := 0; i < len(sorted)-1; i++ {
		if sorted[i] == sorted[i+1] {
			return nil, fmt.Errorf("duplicate member name '%s' in maglevhash.New", sorted[i])
		}
	}

	N := len(sorted)
	M := nextPrime(N * memberTableFactor)

	r := &MaglevHasher{
		members: len(sorted),
		binSize: math.MaxUint64 / uint64(M),
		table:   make([][]string, M),
	}
	for i := range r.table {
		r.table[i] = make([]string, 0, replicas) // preallocate
	}
	if len(members) == 0 {
		return r, nil
	}

	offsets := make([]int, N)
	skips := make([]int, N)
	nextIdxs := make([]int, N)
	for i, m := range sorted {
		v1 := fnv1a.HashString32(m)
		h2 := sha1.Sum([]byte(m))
		v2 := fnv1a.HashBytes32(h2[:])
		offsets[i] = int(v1) % M
		skips[i] = (int(v2) % (M - 1)) + 1
	}
	done := 0
	// fillLoop:
	for done < len(r.table) {
		for i, name := range sorted {
			// next preference for member i
			p := (offsets[i] + nextIdxs[i]*skips[i]) % M
			nextIdxs[i]++

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

// Dispatch gets the members corresponding to i. At evenly distributes all
// possible unsigned integers i across members.
func (m *MaglevHasher) Dispatch(i uint64) []string { return m.table[i/m.binSize] }
func (m *MaglevHasher) Len() int                   { return m.members }
