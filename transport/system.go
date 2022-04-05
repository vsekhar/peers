package transport

import (
	"fmt"
	"net"

	reuse "github.com/libp2p/go-reuseport"
)

type sysTransport struct {
	net.Listener
	*net.Dialer
	*net.UDPConn
}

func (s *sysTransport) Close() error {
	e1 := s.Listener.Close()
	e2 := s.UDPConn.Close()
	if e1 != nil || e2 != nil {
		return fmt.Errorf("transport: tcp %w, udp %v", e1, e2)
	}
	return nil
}

func (s *sysTransport) LocalAddr() net.Addr {
	return s.UDPConn.LocalAddr() // disambiguate *net.Dialer and net.udpConn
}

func System(address string) (Interface, error) {
	tcp, err := reuse.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	pconn, err := reuse.ListenPacket("udp", tcp.Addr().String())
	if err != nil {
		tcp.Close()
		return nil, err
	}
	if udp, ok := pconn.(*net.UDPConn); ok {
		return &sysTransport{
			Listener: tcp,
			Dialer:   new(net.Dialer),
			UDPConn:  udp,
		}, nil
	}
	panic("transport: packet socket is not a UDP socket")
}
