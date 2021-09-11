package quicmux_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"

	"github.com/vsekhar/peers/internal/quicmux"
	"github.com/vsekhar/peers/internal/testtls"
)

var (
	address = "localhost:0"
	tag     = "testtag"
	payload = []byte("test1235")
)

var tlsConfig *tls.Config

func init() {
	tlsConfig = testtls.GenerateTLSConfig()
}

func makeMux(t *testing.T) (m *quicmux.Mux, cancel func()) {
	var ctx context.Context
	var err error
	ctx, cancel = context.WithCancel(context.Background())
	m, err = quicmux.New(ctx, address, tlsConfig)
	if err != nil {
		t.Fatal(err)
	}
	return m, cancel
}

func TestQuicMux(t *testing.T) {
	m, cancel := makeMux(t)
	defer cancel()
	defer m.Close()
	network := m.NewNetwork(tag)
	l, err := network.Listen()
	if err != nil {
		t.Fatal(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(2)

	var sendC, recvC net.Conn

	// Send
	go func() {
		defer wg.Done()
		var err error
		sendC, err = l.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		_, err = sendC.Write(payload)
		if err != nil {
			t.Error(err)
			return
		}
	}()

	// Recv
	go func() {
		defer wg.Done()
		var err error
		recvC, err = network.Dial(l.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}
		defer recvC.Close()
		r := make([]byte, len(payload))
		_, err = recvC.Read(r)
		if err != nil {
			t.Error(err)
			return
		}
		if !bytes.Equal(r, payload) {
			t.Errorf("payloads do not match")
			return
		}
	}()
	wg.Wait()
	recvC.Close()
	sendC.Close()
}

func TestUDP(t *testing.T) {
	m, cancel := makeMux(t)
	defer cancel()
	defer m.Close()

	n1 := m.NewNetwork("network1")
	// n2 := m.NewNetwork("network2")

	sendC, err := n1.ListenPacket()
	if err != nil {
		t.Fatal(err)
	}
	defer sendC.Close()
	recvC, err := n1.ListenPacket()
	if err != nil {
		t.Fatal(err)
	}
	defer recvC.Close()
	_, err = sendC.WriteTo(payload, recvC.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1000)
	n, _, err := recvC.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Errorf("payloads do not match: expected %v, got %v", payload, buf[:n])
	}
}
