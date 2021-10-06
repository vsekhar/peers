package maglevhash

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/vsekhar/peers/dispatch"
	"gonum.org/v1/gonum/stat"
)

const benchmarkReplicas = 3
const testReplicas = 3

var members []string
var probes []string

func init() {
	members = make([]string, 1000)
	for i := range members {
		members[i] = fmt.Sprintf("member_%d", i)
	}
	probes = make([]string, 100000)
	for i := range probes {
		probes[i] = fmt.Sprintf("probe_%d", i)
	}
}

func BenchmarkNextPrime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		nextPrime(i)
	}
}

func BenchmarkCoarsePrimeSafety(b *testing.B) {
	for i := 0; i < 10; i++ {
		b.Run(fmt.Sprintf("coarsePrimeSafety==%d", 1<<i), func(b *testing.B) {
			for j := 0; j < b.N; j++ {
				nextPrimeImpl(j, 1<<i)
			}
		})
	}
}

func BenchmarkNewIterations(b *testing.B) {
	// goos: linux
	// goarch: amd64
	// pkg: github.com/vsekhar/peers/internal/maglevhash
	// cpu: Intel(R) Xeon(R) W-2135 CPU @ 3.70GHz
	// BenchmarkNewIterations-12    	       3	 418120161 ns/op	 9662330 B/op	  135318 allocs/op

	for i := 0; i < b.N; i++ {
		_, err := New(members, benchmarkReplicas)
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
	// BenchmarkNewMembers/1_members-12         	     344	   3206013 ns/op	  178398 B/op	    4337 allocs/op
	// BenchmarkNewMembers/10_members-12        	     212	   5202029 ns/op	  245953 B/op	    5229 allocs/op
	// BenchmarkNewMembers/50_members-12        	      63	  18446212 ns/op	  775608 B/op	   12426 allocs/op
	// BenchmarkNewMembers/100_members-12       	      39	  34675263 ns/op	 1368225 B/op	   20616 allocs/op
	// BenchmarkNewMembers/1000_members-12      	       3	 397203459 ns/op	 9662330 B/op	  135318 allocs/op

	memCount := []int{1, 10, 50, 100, 1000}
	for _, c := range memCount {
		b.Run(fmt.Sprintf("%d_members", c), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := New(members[:c], benchmarkReplicas)
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
	// BenchmarkAt-12    	38646308	        30.92 ns/op	       0 B/op	       0 allocs/op

	m, err := New(members, benchmarkReplicas)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Dispatch(uint64(i))
	}
}

func BenchmarkString(b *testing.B) {
	// goos: linux
	// goarch: amd64
	// pkg: github.com/vsekhar/peers/internal/maglevhash
	// cpu: Intel(R) Xeon(R) W-2135 CPU @ 3.70GHz
	// BenchmarkString-12    	17135240	        69.97 ns/op	       0 B/op	       0 allocs/op

	m, err := New(members, benchmarkReplicas)
	if err != nil {
		b.Fatal(err)
	}
	s := dispatch.ByString(m)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.DispatchString("testing")
	}
}

func BenchmarkGetWithCoalescingSubset(b *testing.B) {
	// goos: linux
	// goarch: amd64
	// pkg: github.com/vsekhar/peers/internal/maglevhash
	// cpu: Intel(R) Xeon(R) W-2135 CPU @ 3.70GHz
	// BenchmarkGetWithCoalescingSubset-12    	15506047	        77.09 ns/op	       0 B/op	       0 allocs/op

	m, err := New(members, benchmarkReplicas)
	if err != nil {
		b.Fatal(err)
	}
	c := dispatch.Coalesce(m)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.CoalescingDispatch("client", "testing", 4)
	}
}

func TestHash(t *testing.T) {
	m, err := New(members[:1], testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	s := dispatch.ByString(m)
	r := s.DispatchString("jacket")
	if r[0] != members[0] {
		t.Error("did not get expected member (one member case)")
	}
	if len(r) != testReplicas {
		t.Errorf("expected %d replicas, got %d", testReplicas, len(r))
	}
}

func TestEmpty(t *testing.T) {
	m, err := New(nil, testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	s := dispatch.ByString(m)
	r := s.DispatchString("test")
	if !equalStringSlice(r, []string{}) {
		t.Errorf("expected empty slice, got %v", r)
	}
	r = m.Dispatch(1)
	if err != nil {
		t.Fatal(err)
	}
	if !equalStringSlice(r, []string{}) {
		t.Errorf("expected empty slice, got %v", r)
	}
}

func TestCoverage(t *testing.T) {
	const N = 100
	m, err := New(members[:N], testReplicas)
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
	m, err := New(members, testReplicas)
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
	m, err := New(members, testReplicas)
	if err != nil {
		t.Fatal(err)
	}

	entryCounts := make(map[string]int)
	for _, entryMembers := range m.table {
		entryCounts[entryMembers[0]]++
	}
	if len(entryCounts) != len(members) {
		t.Errorf("bad distribution: some members do not appear first")
	}
	entryCountsFloat := make([]float64, 0, len(entryCounts))
	for _, count := range entryCounts {
		entryCountsFloat = append(entryCountsFloat, float64(count))
	}
	var (
		targetMean    = float64(len(m.table) / len(members))
		meanTolerance = 1.0
		maxStdDev     = 10.0
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
	s1, s2 := dispatch.ByString(m1), dispatch.ByString(m2)
	if !equalStringSlice(s1.DispatchString("test123"), s2.DispatchString("test123")) {
		t.Error("results are not deterministic")
	}
	if m1.Len() != m2.Len() {
		t.Fatal("member counts are not the same")
	}
	for i := range m1.table {
		if !equalStringSlice(m1.table[i], m2.table[i]) {
			t.Fatal("tables do not match")
		}
	}
}

func TestIntDistribution(t *testing.T) {
	N := 100
	m, err := New(members[:N], testReplicas)
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
		names[m.Dispatch(p)[0]]++
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
		maxStdDev     = 20.0
	)
	testStat(t, counts, targetMean, meanTolerance, minStdDev, maxStdDev)
}

func TestStringDistribution(t *testing.T) {
	N := 100
	m, err := New(members[:N], testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	s := dispatch.ByString(m)
	probesPerMember := 1000
	probeCount := N * probesPerMember
	names := make(map[string]int)
	for _, p := range probes[:probeCount] {
		names[s.DispatchString(p)[0]]++
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

func TestStableHash(t *testing.T) {
	const (
		delta    = 10    // members removed
		maxDiffs = 0.007 // 0.7% changed entries per member removed
	)
	m1, err := New(members, testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := New(members[:len(members)-delta], testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	diffs := 0
	for i, ms := range m1.table {
		for j, m := range ms {
			if m != m2.table[i][j] {
				diffs++
			}
		}
	}
	pdiffs := float64(diffs) / float64(len(m1.table)) / float64(delta)
	if pdiffs > maxDiffs {
		t.Errorf("expected <%f%% differences, got %f%% differences", maxDiffs*100, pdiffs*100)
	}
}

func TestBigInt(t *testing.T) {
	m, err := New(members, testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	m.Dispatch(1 << 31)
	m.Dispatch(1041972534)
}

func TestCoalescing(t *testing.T) {
	m, err := New(members, testReplicas)
	if err != nil {
		t.Fatal(err)
	}
	c := dispatch.Coalesce(m)

	// Return the same members regardless of inputs at level==0
	s1 := c.CoalescingDispatch("abc", "123", 0)
	s2 := c.CoalescingDispatch("123", "abc", 0)
	if !equalStringSlice(s1, s2) {
		t.Errorf("members don't match: %v, %v", s1, s2)
	}

	if c.MaxBits() != 10 {
		// 1000 members is covered by 2^10==1024 bits
		t.Errorf("expected 10, got %d", c.MaxBits())
	}

	clientName := "test123"

	// test stats
	for _, bits := range []int{0, 1, 4, 8} {
		t.Run(fmt.Sprintf("bits_%d", bits), func(t *testing.T) {
			names := make(map[string]int)
			for _, p := range probes {
				names[c.CoalescingDispatch(clientName, p, bits)[0]]++
			}
			uniquesDiff := len(names) - 1<<bits
			if uniquesDiff < 0 {
				uniquesDiff = -uniquesDiff
			}
			if uniquesDiff > (1 << bits / 8) {
				t.Errorf("expected within 12.5%% of %d unique names, got %d", 1<<bits, len(names))
			}
			counts := make([]float64, 0, len(names))
			for _, c := range names {
				counts = append(counts, float64(c))
			}
			var (
				targetMean    = float64(len(probes) / (1 << bits))
				meanTolerance = 55.0
				minStdDev     = 0.0
				maxStdDev     = 1000.0
			)
			testStat(t, counts, targetMean, meanTolerance, minStdDev, maxStdDev)
		})
	}
}
