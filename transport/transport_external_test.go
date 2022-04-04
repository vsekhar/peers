package transport_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/vsekhar/peers/internal/testlog"
	"github.com/vsekhar/peers/internal/testtls"
	"github.com/vsekhar/peers/transport"
)

const payload = "testpayload"

func exchangeTCP(t *testing.T, t1, t2 transport.Interface) {
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		c, err := t1.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		buf := make([]byte, len(payload))
		n, err := c.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
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
		a := t1.Addr()
		c, err := t2.DialContext(context.Background(), a.Network(), a.String())
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

func exchangeUDP(t *testing.T, u1, u2 transport.Interface) {
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, err := u1.WriteTo([]byte(payload), u2.LocalAddr())
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
		n, _, err := u2.ReadFrom(buf)
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
	exchangeTCP(t, s, s)
	exchangeUDP(t, s, s)
}

func TestSplit(t *testing.T) {
	sysTrans, err := transport.System(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, sysTrans.Close)
	tlsTrans := transport.TLSWithInsecureUDP(sysTrans, testtls.Config())
	defer check(t, tlsTrans.Close)
	logger := testlog.New()
	splitTrans := transport.Split(tlsTrans, logger.Std(), "test1", "test2")
	defer func() {
		for _, st := range splitTrans {
			check(t, st.Close)
		}
	}()
	exchangeTCP(t, splitTrans["test1"], splitTrans["test1"])
	exchangeUDP(t, splitTrans["test1"], splitTrans["test1"])
	exchangeUDP(t, splitTrans["test2"], splitTrans["test2"])
	logger.ErrorIfNotEmpty(t)
}

func TestTLSInsecureUDP(t *testing.T) {
	s, err := transport.System(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, s.Close)
	tlsTrans := transport.TLSWithInsecureUDP(s, testtls.Config())
	exchangeTCP(t, tlsTrans, tlsTrans)
	exchangeUDP(t, tlsTrans, tlsTrans)
}

func TestQUIC(t *testing.T) {
	s, err := transport.System(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, s.Close)
	q := transport.QUIC(s, testtls.Config())
	exchangeTCP(t, q, q)
	exchangeUDP(t, q, q)
}

func TestTrace(t *testing.T) {
	s, err := transport.System(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, s.Close)
	l := testlog.New()
	tracer := transport.Trace(s, l.Std())
	defer check(t, tracer.Close)
	exchangeTCP(t, tracer, tracer)
	exchangeUDP(t, tracer, tracer)
	l.ErrorIfEmpty(t)
	l.ErrorIfNotContainsAnyOf(t, []string{
		"closing",
		"dialed",
		"accepted",
		"Read(",
		"Write(",
		"WriteTo",
		"ReadFrom",
	}...)
}
