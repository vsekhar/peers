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
	replicas int
	members  []string
	table    [][]string // table[entryNo][replicaNo]
	binSize  uint64
}

func fill(key string, names []string, table [][]string, replicas int, unique bool) {
	if replicas < 1 {
		panic("replicas must be one or greater")
	}

	N := len(names)
	M := len(table)
	offsets := make([]int, N)
	skips := make([]int, N)
	nextIdxs := make([]int, N)
	for i, m := range names {
		v1 := fnv1a.HashString32(key + m)
		h2 := sha1.Sum([]byte(key + m))
		v2 := fnv1a.HashBytes32(h2[:])
		offsets[i] = int(v1) % M
		skips[i] = (int(v2) % (M - 1)) + 1
	}
	done := 0
	used := make(map[string]struct{})
	// fillLoop:
	for done < len(table) {
		for i, name := range names {
			if unique {
				if _, ok := used[name]; ok {
					continue
				}
			}

			// next preference for member i
			p := (offsets[i] + nextIdxs[i]*skips[i]) % M
			nextIdxs[i]++

			if len(table[p]) < replicas {
				table[p] = append(table[p], name)
				used[name] = struct{}{}
				if len(table[p]) == replicas {
					done++
				}
			}
		}
	}
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
		table:    make([][]string, M),
	}
	for i := range r.table {
		r.table[i] = make([]string, 0, replicas) // preallocate
	}
	if len(members) > 0 {
		fill("", sorted, r.table, replicas, false)
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
	// f := fnv.New64a()
	// f.Write([]byte(value))
	// s := f.Sum64()              // 213ns / op
	s := fnv1a.HashString64(value) // 63ns / op
	return m.At(s)
}

// At gets the members corresponding to i. At evenly distributes all possible
// unsigned integers i across members.
func (m *MaglevHasher) At(i uint64) []string {
	idx := i / m.binSize
	return m.table[idx]
}

// GetWithCoalescingSubset gets a set of members corresponding to clientName
// value for a given coalescing level.
//
// GetWithCoalescingSubset will consider clientName and value up to bits, which
// must be in [0, 64]. If bits is zero, GetWithCoalescingSubset always returns
// the same members for all clientName and value. Otherwise,
// GetWithCoalescingSubset returns at most one of 2^bits possible sets of
// members.
//
// Passing a value for bits in excess of that returned by MaxBits does not
// increase the number of possible return values.
func (m *MaglevHasher) GetWithCoalescingSubset(clientName, value string, bits int) []string {

	// NB: we cannot use common methods like deterministic subsetting[1]
	// because we also need to coalesce around a single set of root nodes.
	//
	// [1]: https://sre.google/sre-book/load-balancing-datacenter

	clientHash := fnv1a.HashString64(clientName)
	valueHash := fnv1a.HashString64(value)

	// Derive key with bits low-order bits of clientHash (C), and bits low-order
	// bits of valueHash (V) in the highest possible position.
	//
	//   E.g. bits==4 --> clientHash == _____CCCC
	//                    valueHash  == _____VVVV
	//                    xor        == _____XXXX
	//                    key        == XXXX0000....

	mask := uint64(math.MaxUint64) >> (64 - bits)
	clientHash &= mask
	valueHash &= mask
	clientHash <<= (64 - bits)
	valueHash <<= (64 - bits)
	key := clientHash ^ valueHash
	return m.At(key)
}

// MaxBits returns the number of bits.
func (m *MaglevHasher) MaxBits() int {
	return int(math.Ceil(math.Log2(float64(m.Len()))))
}

func (m *MaglevHasher) Len() int {
	return len(m.members)
}

func (m *MaglevHasher) dump() string {
	b := &bytes.Buffer{}
	for i, memNo := range m.table {
		fmt.Fprintf(b, "%d: %s\n", i, memNo)
	}
	return b.String()
}
