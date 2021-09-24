package testtls_test

import (
	"crypto/tls"
	"sync"
	"testing"

	"github.com/vsekhar/peers/internal/testtls"
)

const payload = "hello"

func BenchmarkTLS(b *testing.B) {
	// goos: linux
	// goarch: amd64
	// pkg: github.com/vsekhar/peers
	// cpu: Intel(R) Xeon(R) W-2135 CPU @ 3.70GHz
	// BenchmarkTLS-12    	      21	  61607442 ns/op	 1107596 B/op	    6459 allocs/op
	for i := 0; i < b.N; i++ {
		_ = testtls.GenerateTLSConfig()
	}
}

func TestTestTLS(t *testing.T) {
	tcfg := testtls.GenerateTLSConfig()
	l, err := tls.Listen("tcp", ":0", tcfg)
	if err != nil {
		t.Fatal(err)
	}
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		c, err := l.Accept()
		if err != nil {
			t.Error(err)
		}
		if _, err := c.Write([]byte(payload)); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		defer wg.Done()
		c, err := tls.Dial(l.Addr().Network(), l.Addr().String(), tcfg)
		if err != nil {
			t.Error(err)
		}
		p := make([]byte, len(payload))
		n, err := c.Read(p)
		if err != nil {
			t.Error(err)
		}
		if n != len(payload) {
			t.Error("short read")
		}
	}()
	wg.Wait()
}
