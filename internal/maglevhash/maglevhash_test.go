package maglevhash

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"gonum.org/v1/gonum/stat"
)

const benchmarkReplicas = 3
const testReplicas = 3

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
	// BenchmarkNewIterations-12    	      54	  21714886 ns/op	  901007 B/op	   14098 allocs/op

	m := makeMembers("membername", 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := New(m, benchmarkReplicas)
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
	// BenchmarkNewMembers/1_members-12         	     386	   2915512 ns/op	  172403 B/op	    4191 allocs/op
	// BenchmarkNewMembers/10_members-12        	     264	   4515776 ns/op	  238576 B/op	    5143 allocs/op
	// BenchmarkNewMembers/50_members-12        	      97	  12707567 ns/op	  529828 B/op	    9083 allocs/op
	// BenchmarkNewMembers/100_members-12       	      51	  21289694 ns/op	  900928 B/op	   14098 allocs/op
	// BenchmarkNewMembers/1000_members-12      	       4	 281851129 ns/op	 7406030 B/op	  104083 allocs/op	    4080 allocs/op

	memCount := []int{1, 10, 50, 100, 1000}
	for _, c := range memCount {
		b.Run(fmt.Sprintf("%d_members", c), func(b *testing.B) {
			m := makeMembers("membername", c)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := New(m, benchmarkReplicas)
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
	// BenchmarkAt-12    	37780587	        31.03 ns/op	       0 B/op	       0 allocs/op

	m, err := New(makeMembers("membername", 100), benchmarkReplicas)
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
	// BenchmarkGet-12    	23996768	        49.94 ns/op	       0 B/op	       0 allocs/op

	mems := makeMembers("membername", 100)
	m, err := New(mems, benchmarkReplicas)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get("testing")
	}
}

func TestHash(t *testing.T) {
	m, err := New([]string{"hello"}, testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	r := m.Get("jacket")
	if r[0] != "hello" {
		t.Error("did not get expected member (one member case)")
	}
	if len(r) != testReplicas {
		t.Errorf("expected %d replicas, got %d", testReplicas, len(r))
	}
}

func TestCoverage(t *testing.T) {
	const N = 100
	m, err := New(makeMembers("membername", N), testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.table) < N*100 {
		t.Fatalf("expected table size of at least %d, got table size of %d", N*100, len(m.table))
	}

	// Ensure there is a member in every table entry
	for i, e := range m.table {
		if len(e) == 0 {
			t.Errorf("no entry at position %d", i)
		}
	}
}

func TestDump(t *testing.T) {
	t.Helper()
	m, err := New(makeMembers("membername", 10), 3)
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

func TestTable(t *testing.T) {
	m, err := New(makeMembers("membername", 100), testReplicas)
	if err != nil {
		t.Fatal(err)
	}

	for i, replicas := range m.table {
		if len(replicas) != testReplicas {
			t.Errorf("table entry %d, expected %d replicas, got %d replicas", i, testReplicas, len(replicas))
		}
		uniqueR := make(map[string]struct{})
		for _, r := range replicas {
			uniqueR[r] = struct{}{}
		}
		if len(uniqueR) != len(replicas) {
			t.Errorf("table entry %d, expected unique replicas, got %v", i, replicas)
		}
	}
}

func TestFirstReplicaStats(t *testing.T) {
	m, err := New(makeMembers("membername", 100), testReplicas)
	if err != nil {
		t.Fatal(err)
	}

	entryCounts := make(map[string]int)
	for _, memNo := range m.table {
		entryCounts[memNo[0]]++
	}
	if len(entryCounts) != len(m.members) {
		t.Errorf("bad distribution: some members do not appear first")
	}
	entryCountsFloat := make([]float64, 0, len(entryCounts))
	for _, count := range entryCounts {
		entryCountsFloat = append(entryCountsFloat, float64(count))
	}
	const (
		targetMean    = 100.0
		meanTolerance = 0.1
		maxStdDev     = 8.0
	)
	testStat(t, entryCountsFloat, targetMean, meanTolerance, 0, maxStdDev)
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestDeterministic(t *testing.T) {
	members := makeMembers("membername", 100)
	m1, err := New(members, testReplicas)
	if err != nil {
		t.Fatal(err)
	}

	rand.Seed(42)
	rand.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })

	m2, err := New(members, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !equalStringSlice(m1.Get("test123"), m2.Get("test123")) {
		t.Error("results are not deterministic")
	}
	for i := range m1.members {
		if m1.members[i] != m2.members[i] {
			t.Fatal("members not sorted the same")
		}
	}
	for i := range m1.table {
		if !equalStringSlice(m1.table[i], m2.table[i]) {
			t.Fatal("tables do not match")
		}
	}
}

func TestAtDistribution(t *testing.T) {
	N := 100
	m, err := New(makeMembers("membername", N), testReplicas)
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
		names[m.At(p)[0]]++
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
	m, err := New(makeMembers("membername", 100), testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	probesPerMember := 1000
	probeCount := N * probesPerMember
	probes := makeMembers("probename", probeCount) // unrelated to member names
	names := make(map[string]int)
	for _, p := range probes {
		names[m.Get(p)[0]]++
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
	m, err := New(makeMembers("membername", 100), testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	m.At(1 << 31)
	m.At(1041972534)
}
