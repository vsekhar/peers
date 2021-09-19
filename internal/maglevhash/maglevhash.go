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

// TODO: implement backups (populate an array of members at each location in
// the table?)

// TODO: implement stable subsetting
//   Concatenate n-length prefixes of hash of client_id (abcd...) and value
//   (ABCD...):
//
//     Maglev(abcdABCD)
//     Maglev(abcABC)
//     Maglev(abAB)
//     Maglev(aA)
//     Maglev(-)
//
//   We have membership, so we can use number of members to determine the
//   starting value of n. E.g. if we want to have ~16 inbound cxns per host,
//   and we have 1024 hosts, we can

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
	replicas int
	members  []string
	table    [][]string // table[entryNo][replicaNo]
	binSize  uint64
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
	M := nextPrime(N * 100)

	r := &MaglevHasher{
		replicas: replicas,
		members:  sorted,
		binSize:  math.MaxUint64 / uint64(M),
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

	r.table = make([][]string, M)
	for i := range r.table {
		r.table[i] = make([]string, 0, replicas) // preallocate
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

// Replicas returns the number of replicas for each value in the hasher.
func (m *MaglevHasher) Replicas() int {
	return m.replicas
}

// Get returns the members corresponding to value. Get takes care of evenly
// distributing values across members.
func (m *MaglevHasher) Get(value string) []string {
	s := fnv1a.HashString64(value) // faster than stdlib fnv (63ns vs 213ns)
	return m.At(s)
}

// At gets the members corresponding to i. At evenly distributes all possible
// unsigned integers i across members.
func (m *MaglevHasher) At(i uint64) []string {
	idx := i / m.binSize
	return m.table[idx]
}

func (m *MaglevHasher) dump() string {
	b := &bytes.Buffer{}
	for i, memNo := range m.table {
		fmt.Fprintf(b, "%d: %s\n", i, memNo)
	}
	return b.String()
}
