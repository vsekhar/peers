package maglevhash

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"gonum.org/v1/gonum/stat"
)

func BenchmarkNextPrime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		nextPrime(i)
	}
}

func makeMembers(n int) []string {
	r := make([]string, n)
	for i := range r {
		r[i] = fmt.Sprintf("maglevhash_test_member_%d", i)
	}
	return r
}
func BenchmarkNewIterations(b *testing.B) {
	m := makeMembers(100)
	for i := 0; i < b.N; i++ {
		_, err := New(m)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewMembers(b *testing.B) {
	// This seems to run forever and leak memory
	memCount := []int{1, 10, 50, 100, 1000}
	for _, c := range memCount {
		b.Run(fmt.Sprintf("%d_members", c), func(b *testing.B) {
			m := makeMembers(c)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := New(m)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	mems := makeMembers(100)
	m, err := New(mems)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get("testing")
	}
}

func TestHash(t *testing.T) {
	m, err := New([]string{"hello"})
	if err != nil {
		t.Fatal(err)
	}
	if m.Get("jacket") != "hello" {
		t.Error("blah")
	}
}

func TestCoverage(t *testing.T) {
	const N = 100
	m, err := New(makeMembers(N))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.table) < N*100 {
		t.Fatalf("expected table size of at least %d, got table size of %d", N*100, len(m.table))
	}

	// Ensure there is a member in every table entry
	for i, e := range m.table {
		if e < 0 {
			t.Errorf("no entry at position %d", i)
		}
		if int(e) >= len(m.members) {
			t.Errorf("no matching member at table position %d (member #%d)", i, e)
		}
	}
}

func testDump(t *testing.T) {
	m, err := New(makeMembers(10))
	if err != nil {
		t.Fatal(err)
	}
	t.Error(m.dump())
}

func TestStats(t *testing.T) {
	m, err := New(makeMembers(100))
	if err != nil {
		t.Fatal(err)
	}
	entryCounts := make(map[int]int)
	for _, memNo := range m.table {
		entryCounts[int(memNo)]++
	}
	if len(entryCounts) != len(m.members) {
		t.Errorf("some members have no entries")
	}
	entryCountsFloat := make([]float64, len(entryCounts))
	for memNo, count := range entryCounts {
		entryCountsFloat[memNo] = float64(count)
	}
	stddev := stat.StdDev(entryCountsFloat, nil)
	mean := stat.Mean(entryCountsFloat, nil)
	const maxStdDev = 8.0
	if stddev > maxStdDev {
		t.Errorf("Standard deviation of entries per member is too high: %f +/- %f (expected +/- <%f)", mean, stddev, maxStdDev)
	}
}

func TestDeterministic(t *testing.T) {
	members := makeMembers(100)
	m1, err := New(members)
	if err != nil {
		t.Fatal(err)
	}

	// Scramble things in case there is any use of rand
	rand.Seed(time.Now().UnixNano())

	m2, err := New(members)
	if err != nil {
		t.Fatal(err)
	}
	if m1.Get("test123") != m2.Get("test123") {
		t.Error("results are not deterministic")
	}
	for i := range m1.members {
		if m1.members[i] != m2.members[i] {
			t.Fatal("members not sorted the same")
		}
	}
	for i := range m1.table {
		if m1.table[i] != m2.table[i] {
			t.Fatal("tables do not match")
		}
	}
}
