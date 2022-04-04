package transport

import (
	"context"
	"net"
	"syscall"
)

type Dialer interface {
	Dial(network, address string) (net.Conn, error)
}

type ContextDialer interface {
	Dialer
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// udpConn is the minimum interface satisfied by net.UDPConn and needed by QUIC.
type udpConn interface {
	net.PacketConn
	SetReadBuffer(int) error               // for QUIC
	SyscallConn() (syscall.RawConn, error) // for QUIC
}

type Interface interface {
	net.Listener
	ContextDialer
	udpConn
}
