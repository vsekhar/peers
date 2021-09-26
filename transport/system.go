package transport

import (
	"net"

	reuse "github.com/libp2p/go-reuseport"
)

type sysTransport struct {
	net.Listener
	*net.Dialer
}

func System(network, address string) (Interface, error) {
	l, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	return &sysTransport{
		Listener: l,
		Dialer:   new(net.Dialer),
	}, nil
}

func SystemTCPUDP(address string) (Interface, net.PacketConn, error) {
	// TODO: after implementing UDP muxing, SystemTCPUDP will become System.

	tcp, err := reuse.Listen("tcp", address)
	if err != nil {
		return nil, nil, err
	}
	udp, err := reuse.ListenPacket("udp", tcp.Addr().String())
	if err != nil {
		tcp.Close()
		return nil, nil, err
	}
	return &sysTransport{
		Listener: tcp,
		Dialer:   new(net.Dialer),
	}, udp, nil
}
