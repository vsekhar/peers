package transport

import (
	"context"
	"crypto/tls"
	"net"
)

type tlsTransport struct {
	Interface
	config *tls.Config
}

func (t *tlsTransport) Dial(network, address string) (net.Conn, error) {
	return t.DialContext(context.Background(), network, address)
}

func (t *tlsTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := t.Interface.DialContext(ctx, network, address)
	if err != nil {
		return c, err
	}
	tc := tls.Client(c, t.config)
	return tc, nil
}

func (t *tlsTransport) Accept() (net.Conn, error) {
	c, err := t.Interface.Accept()
	if err != nil {
		return c, err
	}
	tc := tls.Server(c, t.config)
	return tc, nil
}

// TODO: override PacketConn methods to implement symmetric encryption?

func TLSWithInsecureUDP(transport Interface, config *tls.Config) Interface {
	return &tlsTransport{
		Interface: transport,
		config:    config,
	}
}
