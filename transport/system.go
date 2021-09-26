package transport

import (
	"fmt"
	"net"

	reuse "github.com/libp2p/go-reuseport"
)

type sysTransport struct {
	net.Listener
	*net.Dialer
	net.PacketConn
}

func (s *sysTransport) Close() error {
	e1 := s.Listener.Close()
	e2 := s.PacketConn.Close()
	if e1 != nil || e2 != nil {
		return fmt.Errorf("transport: tcp %w, udp %v", e1, e2)
	}
	return nil
}

func (s *sysTransport) LocalAddr() net.Addr {
	// TODO: why is this needed? Embedded PacketConn provides LocalAddr...
	return s.PacketConn.LocalAddr()
}

func System(address string) (Interface, error) {
	tcp, err := reuse.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	udp, err := reuse.ListenPacket("udp", tcp.Addr().String())
	if err != nil {
		tcp.Close()
		return nil, err
	}
	return &sysTransport{
		Listener:   tcp,
		Dialer:     new(net.Dialer),
		PacketConn: udp,
	}, nil
}
