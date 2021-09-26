package transport_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/vsekhar/peers/internal/testlog"
	"github.com/vsekhar/peers/internal/testtls"
	"github.com/vsekhar/peers/transport"
)

const payload = "testpayload"

func exchangePayloads(t *testing.T, trans transport.Interface) {
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

func exchangeUDP(t *testing.T, udp net.PacketConn) {
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
	s, err := transport.System("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, s.Close)
	exchangePayloads(t, s)
}

func TestSystemTCPUDP(t *testing.T) {
	s, udp, err := transport.SystemTCPUDP(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, s.Close)
	exchangePayloads(t, s)
	exchangeUDP(t, udp)
}

func TestTLS(t *testing.T) {
	s, err := transport.System("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, s.Close)
	tlsTrans := transport.TLS(s, testtls.Config())
	exchangePayloads(t, tlsTrans)
}

func TestTagged(t *testing.T) {
	sysTrans, err := transport.System("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer check(t, sysTrans.Close)
	tlsTrans := transport.TLS(sysTrans, testtls.Config())
	defer check(t, tlsTrans.Close)
	logger := testlog.New()
	taggedTrans := transport.Tagged(tlsTrans, logger.Std(), "test1", "test2")
	exchangePayloads(t, taggedTrans["test1"])
	logger.ErrorIfNotEmpty(t)
}
