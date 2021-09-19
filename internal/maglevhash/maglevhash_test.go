package maglevhash

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"gonum.org/v1/gonum/stat"
)

func BenchmarkNextPrime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		nextPrime(i)
	}
}

func makeMembers(prefix string, n int) []string {
	r := make([]string, n)
	for i := range r {
		r[i] = fmt.Sprintf("maglevhash_test_%s_%d", prefix, i)
	}
	return r
}
func BenchmarkNewIterations(b *testing.B) {
	// goos: linux
	// goarch: amd64
	// pkg: github.com/vsekhar/peers/internal/maglevhash
	// cpu: Intel(R) Xeon(R) W-2135 CPU @ 3.70GHz
	// BenchmarkNewIterations-12    	      99	  11787820 ns/op	  256735 B/op	    4091 allocs/op

	m := makeMembers("membername", 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := New(m)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewMembers(b *testing.B) {
	// goos: linux
	// goarch: amd64
	// pkg: github.com/vsekhar/peers/internal/maglevhash
	// cpu: Intel(R) Xeon(R) W-2135 CPU @ 3.70GHz
	// BenchmarkNewMembers/1_members-12         	     430	   2849691 ns/op	  165752 B/op	    4090 allocs/op
	// BenchmarkNewMembers/10_members-12        	     320	   3794577 ns/op	  173761 B/op	    4134 allocs/op
	// BenchmarkNewMembers/50_members-12        	     180	   7241607 ns/op	  207761 B/op	    4080 allocs/op
	// BenchmarkNewMembers/100_members-12       	      98	  11602887 ns/op	  256734 B/op	    4091 allocs/op
	// BenchmarkNewMembers/1000_members-12      	      10	 104412401 ns/op	 1008433 B/op	    4080 allocs/op

	memCount := []int{1, 10, 50, 100, 1000}
	for _, c := range memCount {
		b.Run(fmt.Sprintf("%d_members", c), func(b *testing.B) {
			m := makeMembers("membername", c)
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

func BenchmarkAt(b *testing.B) {
	// goos: linux
	// goarch: amd64
	// pkg: github.com/vsekhar/peers/internal/maglevhash
	// cpu: Intel(R) Xeon(R) W-2135 CPU @ 3.70GHz
	// BenchmarkAt-12    	24299367	        49.18 ns/op	       0 B/op	       0 allocs/op

	m, err := New(makeMembers("membername", 100))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.At(uint64(i))
	}
}

func BenchmarkGet(b *testing.B) {
	// goos: linux
	// goarch: amd64
	// pkg: github.com/vsekhar/peers/internal/maglevhash
	// cpu: Intel(R) Xeon(R) W-2135 CPU @ 3.70GHz
	// BenchmarkGet-12    	18759733	        63.79 ns/op	       0 B/op	       0 allocs/op

	mems := makeMembers("membername", 100)
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
	m, err := New(makeMembers("membername", N))
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
	t.Helper()
	m, err := New(makeMembers("membername", 10))
	if err != nil {
		t.Fatal(err)
	}
	t.Error(m.dump())
}

func testStat(t *testing.T, v []float64, mean, meanTolerance, minStdDev, maxStdDev float64) {
	t.Helper()
	m := stat.Mean(v, nil)
	s := stat.StdDev(v, nil)
	if math.Abs(m-mean) > meanTolerance {
		t.Errorf("mean %f; stdev %f: expected mean within %f of %f", m, s, meanTolerance, mean)
	}
	if s < minStdDev {
		t.Errorf("mean %f; stdev %f: expected stddev >%f", m, s, minStdDev)
	}
	if s > maxStdDev {
		t.Errorf("mean %f; stdev %f: expected stddev <%f", m, s, maxStdDev)
	}
}

func TestTableStats(t *testing.T) {
	m, err := New(makeMembers("membername", 100))
	if err != nil {
		t.Fatal(err)
	}

	tFloat := make([]float64, len(m.table))
	for i := range m.table {
		tFloat[i] = float64(m.table[i])
	}

	const (
		targetMean    = 50.0
		meanTolerance = 1.0
		minStdDev     = 25.0
	)
	testStat(t, tFloat, targetMean, meanTolerance, minStdDev, 1<<32)
}

func TestMemberEntryStats(t *testing.T) {
	m, err := New(makeMembers("membername", 100))
	if err != nil {
		t.Fatal(err)
	}
	entryCounts := make(map[int]int)
	for _, memNo := range m.table {
		entryCounts[memNo]++
	}
	if len(entryCounts) != len(m.members) {
		t.Errorf("some members have no entries")
	}
	entryCountsFloat := make([]float64, len(entryCounts))
	for memNo, count := range entryCounts {
		entryCountsFloat[memNo] = float64(count)
	}
	const (
		targetMean    = 100.0
		meanTolerance = 0.1
		maxStdDev     = 8.0
	)
	testStat(t, entryCountsFloat, targetMean, meanTolerance, 0, maxStdDev)
}

func TestDeterministic(t *testing.T) {
	members := makeMembers("membername", 100)
	m1, err := New(members)
	if err != nil {
		t.Fatal(err)
	}

	rand.Seed(42)
	rand.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })

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

func TestAtDistribution(t *testing.T) {
	N := 100
	m, err := New(makeMembers("membername", N))
	if err != nil {
		t.Fatal(err)
	}
	src := rand.NewSource(42)
	r := rand.New(src)
	probeCount := N * 100
	probes := make([]uint64, probeCount)
	for i := 0; i < N*100; i++ {
		probes[i] = r.Uint64()
	}
	names := make(map[string]int)
	for _, p := range probes {
		names[m.At(p)]++
	}
	counts := make([]float64, 0, len(names))
	for _, v := range names {
		counts = append(counts, float64(v))
	}
	if len(counts) != N {
		t.Errorf("expected %d unique members among data set, got %d", N, len(counts))
	}

	const (
		targetMean    = 100.0
		meanTolerance = 0.1
		minStdDev     = 0
		maxStdDev     = 15.0
	)
	testStat(t, counts, targetMean, meanTolerance, minStdDev, maxStdDev)
}

func TestGetDistribution(t *testing.T) {
	N := 100
	m, err := New(makeMembers("membername", 100))
	if err != nil {
		t.Fatal(err)
	}
	probesPerMember := 1000
	probeCount := N * probesPerMember
	probes := makeMembers("probename", probeCount) // unrelated to member names
	names := make(map[string]int)
	for _, p := range probes {
		names[m.Get(p)]++
	}
	counts := make([]float64, 0, len(names))
	for _, v := range names {
		counts = append(counts, float64(v))
	}
	if len(counts) != N {
		t.Errorf("expected %d unique members among data set, got %d", N, len(counts))
	}
	var (
		targetMean    = float64(probesPerMember)
		meanTolerance = 0.1
		minStdDev     = 0.0
		maxStdDev     = 180.0
	)
	testStat(t, counts, targetMean, meanTolerance, minStdDev, maxStdDev)
}

func TestBigAt(t *testing.T) {
	m, err := New(makeMembers("membername", 100))
	if err != nil {
		t.Fatal(err)
	}
	m.At(1 << 31)
	m.At(1041972534)
}
