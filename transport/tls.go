package transport

import (
	"context"
	"crypto/tls"
	"net"
)

// Securing UDP is a difficult problem since it requires key exchange and
// shared state (e.g. a counter for replay resistance). This makes UDP
// connection-oriented at the endpoints, just over UDP rather than TCP (see
// github.com/pion/dtls), which negates some of its benefits as a transport for
// peer discovery and gossip. For this reason, UDP is left unsecured here.

type tlsWithInsecureUDPTransport struct {
	Interface
	config *tls.Config
}

var _ Interface = (*tlsWithInsecureUDPTransport)(nil)

func (t *tlsWithInsecureUDPTransport) Dial(network, address string) (net.Conn, error) {
	return t.DialContext(context.Background(), network, address)
}

func (t *tlsWithInsecureUDPTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := t.Interface.DialContext(ctx, network, address)
	if err != nil {
		return c, err
	}
	tc := tls.Client(c, t.config)
	return tc, nil
}

func (t *tlsWithInsecureUDPTransport) Accept() (net.Conn, error) {
	c, err := t.Interface.Accept()
	if err != nil {
		return c, err
	}
	tc := tls.Server(c, t.config)
	return tc, nil
}

func TLSWithInsecureUDP(transport Interface, config *tls.Config) Interface {
	return &tlsWithInsecureUDPTransport{
		Interface: transport,
		config:    config,
	}
}
