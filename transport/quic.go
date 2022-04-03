package transport

import (
	"context"
	"crypto/tls"
	"net"
	"syscall"
	"time"
)

// This file contains a transport implementation that uses QUIC to secure both
// TCP and UDP traffic.

// QUIC contains two layers of abstraction (session and stream) whereas other
// network protocols contain only one (socket). To handle this, we cache
// QUIC sessions and re-use them when dialing or listening for new streams, and
// we pass streams on to the application to be used as traditional sockets.
// Since we cannot trust network addresses due to NAT, each host creates a GUID
// and sends it to the other on the first stream when creating a session. If
// a new incoming session arrives for a given peer GUID, hosts must terminate
// the old session.

type quicTransport struct {
	// Wrap quicSessionManager to implement Interface.
	sessionMgr *quicSessionManager
	ctx        context.Context
	cancel     func()
}

var _ Interface = (*quicTransport)(nil)

func (q *quicTransport) Addr() net.Addr            { return q.sessionMgr.transport.Addr() }
func (q *quicTransport) LocalAddr() net.Addr       { return q.sessionMgr.transport.LocalAddr() }
func (q *quicTransport) SetReadBuffer(i int) error { return q.sessionMgr.transport.SetReadBuffer(i) }
func (q *quicTransport) SyscallConn() (syscall.RawConn, error) {
	return q.sessionMgr.transport.SyscallConn()
}
func (q *quicTransport) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	return q.sessionMgr.ReadFrom(p)
}
func (q *quicTransport) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	return q.sessionMgr.WriteTo(p, addr)
}
func (q *quicTransport) SetDeadline(t time.Time) error { return q.sessionMgr.transport.SetDeadline(t) }
func (q *quicTransport) SetReadDeadline(t time.Time) error {
	return q.sessionMgr.transport.SetReadDeadline(t)
}
func (q *quicTransport) SetWriteDeadline(t time.Time) error {
	return q.sessionMgr.transport.SetWriteDeadline(t)
}

func (q *quicTransport) Dial(network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(q.ctx, quicTimeout)
	defer cancel()
	return q.DialContext(ctx, network, address)
}

func (q *quicTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return q.sessionMgr.DialContext(ctx, network, address)
}

func (q *quicTransport) Accept() (net.Conn, error) {
	return q.sessionMgr.Accept()
}

func (q *quicTransport) Close() error {
	q.cancel()
	return nil
}

func QUIC(transport Interface, config *tls.Config) Interface {
	ctx, cancel := context.WithCancel(context.Background())
	r := &quicTransport{
		sessionMgr: newQUICSessionManager(ctx, transport, config),
		ctx:        ctx,
		cancel:     cancel,
	}
	return r
}
