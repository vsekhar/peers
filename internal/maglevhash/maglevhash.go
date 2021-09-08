package maglevhash

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"math/big"
	"math/rand"
	"runtime"
	"sort"
	"sync"
)

const coarsePrimeSafety = 5
const finePrimeSafety = 1000

func parDo(start, end int, f func(i int)) {
	wg := sync.WaitGroup{}
	workers := runtime.NumCPU()
	jobsPerWorker := (end - start) / workers
	remainder := (end - start) % workers
	wg.Add(workers + 1)
	for w := 0; w < workers; w++ {
		go func(w int) {
			for i := w * jobsPerWorker; i < ((w + 1) * jobsPerWorker); i++ {
				f(i)
			}
			wg.Done()
		}(w)
	}
	go func() {
		for i := end - remainder; i < end; i++ {
			f(i)
		}
		wg.Done()
	}()
	wg.Wait()
}

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

type MaglevHasher struct {
	members []string
	table   []int // indexes into 'members'
}

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

	N := len(members)
	M := nextPrime(N * 100)

	// permutation tables for each member, seeded by its name
	p := make([][]int, N)
	parDo(0, len(p), func(i int) {
		name := members[i]
		h := fnv.New32()
		h.Write([]byte(name))
		hashedName := h.Sum32()
		permuter := rand.New(rand.NewSource(int64(hashedName)))
		p[i] = make([]int, M)
		for j := range p[i] {
			p[i][j] = j
		}
		permuter.Shuffle(len(p[i]), func(a, b int) { p[i][a], p[i][b] = p[i][b], p[i][a] })
	})

	var nSet int = 0
	r.table = make([]int, M)
	for i := 0; i < M; i++ {
		r.table[i] = -1
	}

	// Cannot parallelize this step, or you get race conditions and end up with
	// a non-deterministic and non-stable result.
entries:
	for i := 0; i < M; i++ {
		for memberNo := range p {
			preference := p[memberNo][i]
			if r.table[preference] == -1 {
				r.table[preference] = memberNo
				nSet++
				if nSet >= M {
					break entries
				}
			}
		}
	}

	return r, nil
}

func (m *MaglevHasher) Get(value string) string {
	h := fnv.New32()
	h.Write([]byte(value))
	s := h.Sum32()
	return m.members[m.table[int(s)%len(m.table)]]
}

func (m *MaglevHasher) dump() string {
	b := &bytes.Buffer{}
	for i, memNo := range m.table {
		fmt.Fprintf(b, "%d: %s\n", i, m.members[memNo])
	}
	return b.String()
}
