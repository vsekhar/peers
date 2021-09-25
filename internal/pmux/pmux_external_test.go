package pmux_test

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"sync"
	"testing"

	"github.com/vsekhar/peers/internal/pmux"
	"github.com/vsekhar/peers/internal/syncbuf"
)

const payload = "testpayload"

func TestPMux(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	lbuf := new(syncbuf.Syncbuf)
	logger := log.New(lbuf, "", log.LstdFlags|log.Lshortfile)
	mux := pmux.New(l, new(net.Dialer), logger, "test1", "test2")
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		c, err := mux["test1"].Accept()
		if err != nil {
			t.Error(err)
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
		a := mux["test1"].Addr()
		c, err := mux["test1"].DialContext(context.Background(), a.Network(), a.String())
		if err != nil {
			t.Error(err)
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
