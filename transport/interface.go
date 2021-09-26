package transport

import (
	"context"
	"net"
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
	// TODO: add net.PacketConn
}
