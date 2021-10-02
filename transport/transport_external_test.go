package transport_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/vsekhar/peers/internal/testlog"
	"github.com/vsekhar/peers/internal/testtls"
	"github.com/vsekhar/peers/transport"
)

const payload = "testpayload"

func exchangeTCP(t *testing.T, trans transport.Interface) {
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		c, err := trans.Accept() // TODO: hangs here
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		buf := make([]byte, len(payload))
		n, err := c.Read(buf)
		if err != nil {
			t.Error(err)
		}
		if n != len(payload) {
			t.Error("short read")
		}
		if !bytes.Equal(buf, []byte(payload)) {
			t.Errorf("doesn't match: %v and %v", buf, []byte(payload))
		}
	}()
	go func() {
		defer wg.Done()
		a := trans.Addr()
		c, err := trans.DialContext(context.Background(), a.Network(), a.String())
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		n, err := c.Write([]byte(payload))
		if err != nil {
			t.Error(err)
		}
		if n != len(payload) {
			t.Error(io.ErrShortWrite)
		}
	}()
	wg.Wait()

}

func exchangeUDP(t *testing.T, udp transport.Interface) {
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, err := udp.WriteTo([]byte(payload), udp.LocalAddr())
		if err != nil {
			t.Error(err)
			return
		}
		if n != len(payload) {
			t.Error(io.ErrShortWrite)
		}
	}()
	go func() {
		defer wg.Done()
		buf := make([]byte, len(payload))
		n, _, err := udp.ReadFrom(buf)
		if err != nil {
			t.Error(err)
			return
		}
		if n != len(payload) {
			t.Errorf("short read")
		}
		if !bytes.Equal(buf, []byte(payload)) {
			t.Errorf("expected %v, got %v", []byte(payload), buf)
		}
	}()
	wg.Wait()
}

func check(t *testing.T, f func() error) {
	if err := f(); err != nil {
		if !strings.Contains(err.Error(), "use of closed network connection") {
			t.Error(err)
		}
	}
}

func TestSystem(t *testing.T) {
	s, err := transport.System(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, s.Close)
	exchangeTCP(t, s)
	exchangeUDP(t, s)
}

func TestTagged(t *testing.T) {
	sysTrans, err := transport.System(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, sysTrans.Close)
	tlsTrans := transport.TLSWithInsecureUDP(sysTrans, testtls.Config())
	defer check(t, tlsTrans.Close)
	logger := testlog.New()
	taggedTrans := transport.Tagged(tlsTrans, logger.Std(), "test1", "test2")
	exchangeTCP(t, taggedTrans["test1"])
	exchangeUDP(t, taggedTrans["test1"])
	exchangeUDP(t, taggedTrans["test2"])
	logger.ErrorIfNotEmpty(t)
}

func TestTLSInsecureUDP(t *testing.T) {
	s, err := transport.System(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, s.Close)
	tlsTrans := transport.TLSWithInsecureUDP(s, testtls.Config())
	exchangeTCP(t, tlsTrans)
	exchangeUDP(t, tlsTrans)
}

func TestTLS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := transport.System(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, s.Close)
	logger := testlog.New()
	tlsTrans := transport.TLS(ctx, s, testtls.Config(), logger.Std())
	exchangeTCP(t, tlsTrans)
	exchangeUDP(t, tlsTrans)
	logger.ErrorIfNotEmpty(t)
}
