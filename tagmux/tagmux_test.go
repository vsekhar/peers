package tagmux_test

import (
	"bytes"
	"net"
	"sync"
	"testing"

	"github.com/vsekhar/peers/tagmux"
)

var (
	network = "tcp"
	address = "0.0.0.0:0"
	tag     = "testtag"
	payload = []byte("test1235")
)

func TestTagMux(t *testing.T) {
	m, err := tagmux.New(network, address)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	l, err := m.Listen(tag)
	if err != nil {
		t.Fatal(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Send
	go func() {
		defer wg.Done()
		c, err := l.Accept()
		if err != nil {
			t.Error(err)
		}
		defer c.Close()
		_, err = c.Write(payload)
		if err != nil {
			t.Error(err)
		}
		c.Close()
	}()

	// Recv
	go func() {
		defer wg.Done()
		c, err := tagmux.Dial(l.Addr().Network(), l.Addr().String(), tag)
		if err != nil {
			t.Error(err)
		}
		defer c.Close()
		r := make([]byte, len(payload))
		_, err = c.Read(r)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(r, payload) {
			t.Errorf("payloads do not match")
		}
	}()
	wg.Wait()
}

func TestListenerClose(t *testing.T) {
	m, err := tagmux.New(network, address)
	if err != nil {
		t.Fatal(err)
	}
	l, err := m.Listen(tag)
	if err != nil {
		t.Fatal(err)
	}
	err = m.Close()
	if err != nil {
		t.Error(err)
	}
	_, err = l.Accept()
	opErr, ok := err.(*net.OpError)
	if !ok {
		t.Fatalf("bad error %v", err)
	}
	if opErr.Err.Error() != "use of closed network connection" {
		t.Errorf("bad error %v", opErr)
	}
}

func TestMultipleListeners(t *testing.T) {
	m, err := tagmux.New(network, address)
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Listen(tag)
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Listen(tag)
	if opErr, ok := err.(*net.OpError); ok {
		if opErr.Err.Error() == "tag in use" {
			return
		}
	}
	t.Errorf("bad error %v", err)
}
