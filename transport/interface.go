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

type Interface interface {
	net.Listener
	ContextDialer
	net.PacketConn
	SetReadBuffer(int) error               // for QUIC
	SyscallConn() (syscall.RawConn, error) // for QUIC
}
