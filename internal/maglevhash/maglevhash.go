// Package maglevhash implements the hash function from Maglev, Google's fast
// load balancer.
//
//   https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/44824.pdf
package maglevhash

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math"
	"math/big"
	"sort"

	"github.com/segmentio/fasthash/fnv1a"
)

func nextPrime(n int) int {
	const (
		coarsePrimeSafety = 5
		finePrimeSafety   = 1000
	)

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

type MaglevHasher struct {
	members []string
	table   []int // indexes into 'members'
	binSize uint64
}

// New creates a new MaglevHasher consisting of members.
func New(members []string) (*MaglevHasher, error) {
	sorted := append([]string(nil), members...)
	sort.Strings(sorted)
	r := &MaglevHasher{
		members: sorted,
	}

	for i := 0; i < len(sorted)-1; i++ {
		if sorted[i] == sorted[i+1] {
			return nil, fmt.Errorf("duplicate member name '%s' in maglevhash.New", sorted[i])
		}
	}

	N := len(sorted)
	M := nextPrime(N * 100)

	// For scaling integer arguments in the At method.
	r.binSize = math.MaxUint64 / uint64(M)

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

	r.table = make([]int, M)
	for i := 0; i < M; i++ {
		r.table[i] = -1
	}
	var nSet int = 0
fillLoop:
	for {
		for i := range sorted {
			// next preference for member i
			p := (offsets[i] + nextIdxs[i]*skips[i]) % M
			nextIdxs[i]++
			if r.table[p] == -1 {
				r.table[p] = i
				nSet++
				if nSet >= len(r.table) {
					break fillLoop
				}
			}
		}
	}
	return r, nil
}

// Get returns the member corresponding to value. Get takes care of evenly
// distributing values across members.
func (m *MaglevHasher) Get(value string) string {
	s := fnv1a.HashString64(value) // faster than stdlib fnv (63ns vs 213ns)
	return m.At(s)
}

// At gets the member corresponding to i. At evenly distributes all possible
// unsigned integers i across members.
func (m *MaglevHasher) At(i uint64) string {
	idx := i / m.binSize
	return m.members[m.table[idx]]
}

func (m *MaglevHasher) dump() string {
	b := &bytes.Buffer{}
	for i, memNo := range m.table {
		fmt.Fprintf(b, "%d: %s\n", i, m.members[memNo])
	}
	return b.String()
}
