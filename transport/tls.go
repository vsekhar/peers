package transport

import (
	"context"
	"crypto/tls"
	"net"
)

type tlsTransport struct {
	t      Interface
	config *tls.Config
}

func (t *tlsTransport) Addr() net.Addr { return t.t.Addr() }
func (t *tlsTransport) Close() error   { return t.t.Close() }

func (t *tlsTransport) Dial(network, address string) (net.Conn, error) {
	return t.DialContext(context.Background(), network, address)
}

func (t *tlsTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := t.t.DialContext(ctx, network, address)
	if err != nil {
		return c, err
	}
	tc := tls.Client(c, t.config)
	return tc, nil
}

func (t *tlsTransport) Accept() (net.Conn, error) {
	c, err := t.t.Accept()
	if err != nil {
		return c, err
	}
	tc := tls.Server(c, t.config)
	return tc, nil
}

func TLS(transport Interface, config *tls.Config) Interface {
	return &tlsTransport{
		t:      transport,
		config: config,
	}
}
