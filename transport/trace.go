package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"syscall"
	"time"
)

type TraceLogger interface {
	Printf(string, ...interface{})
}

type tracingConn struct {
	conn   net.Conn // don't embed, so we can be sure we are logging all the methods
	logger TraceLogger
}

var _ net.Conn = (*tracingConn)(nil)

func (c *tracingConn) log(x string, args ...interface{}) {
	c.logger.Printf("conn(%s-->%s): %s", c.conn.LocalAddr().String(), c.conn.RemoteAddr().String(),
		fmt.Sprintf(x, args...))
}

func (t *tracingConn) Close() error {
	t.log("closing")
	return t.conn.Close()
}

func (t *tracingConn) Read(p []byte) (int, error) {
	t.log("Read(%d bytes)...", len(p))
	n, err := t.conn.Read(p)
	t.log("Read(%x) --> %d, %s", p, n, err)
	return n, err
}

func (t *tracingConn) Write(p []byte) (int, error) {
	t.log("Write(%x)...", p)
	n, err := t.conn.Write(p)
	t.log("Write(%x) --> %d, %s", p, n, err)
	return n, err
}

func (l *tracingConn) SetDeadline(t time.Time) error {
	l.log("setting deadline to %s", t)
	return l.conn.SetDeadline(t)
}

func (l *tracingConn) SetReadDeadline(t time.Time) error {
	l.log("setting read deadline to %s", t)
	return l.conn.SetReadDeadline(t)
}

func (l *tracingConn) SetWriteDeadline(t time.Time) error {
	l.log("setting write deadline to %s", t)
	return l.conn.SetWriteDeadline(t)
}

// Not traced

func (t *tracingConn) LocalAddr() net.Addr  { return t.conn.LocalAddr() }
func (t *tracingConn) RemoteAddr() net.Addr { return t.conn.RemoteAddr() }

type tracingTransport struct {
	transport Interface // don't embed, so we can be sure we are logging all the methods
	logger    TraceLogger
}

var _ Interface = (*tracingTransport)(nil)

func Trace(transport Interface, logger TraceLogger) Interface {
	r := &tracingTransport{
		transport: transport,
		logger:    logger,
	}
	if r.logger == nil {
		r.logger = log.Default()
	}
	return r
}

// TODO: general way to get function name and arguments? via runtime package?

func (l *tracingTransport) log(x string, args ...interface{}) {
	l.logger.Printf("transport(%s): %s", l.transport.LocalAddr().String(),
		fmt.Sprintf(x, args...))
}

func (l *tracingTransport) Dial(network, address string) (net.Conn, error) {
	return l.DialContext(context.Background(), network, address)
}

func (l *tracingTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	l.log("dialing %s:%s...", network, address)
	c, err := l.transport.DialContext(ctx, network, address)
	if err != nil {
		c.Close()
		l.log("dialing %s:%s error: %s", network, address, err)
		return nil, err
	}
	l.log("dialed %s:%s", network, address)
	return &tracingConn{c, l.logger}, err
}

func (l *tracingTransport) Accept() (net.Conn, error) {
	l.log("accepting...")
	c, err := l.transport.Accept()
	if err != nil {
		l.log("accept error: %s", err)
		return nil, err
	} else {
		l.log("accepted from %s", c.RemoteAddr().String())
		return &tracingConn{c, l.logger}, nil
	}
}

func (l *tracingTransport) Close() error {
	l.log("closing")
	return l.transport.Close()
}

func (l *tracingTransport) ReadFrom(p []byte) (int, net.Addr, error) {
	l.log("ReadFrom...")
	n, a, err := l.transport.ReadFrom(p)
	l.log("ReadFrom(%x) --> %d, %s, %s", p, n, a, err)
	return n, a, err
}

func (l *tracingTransport) WriteTo(p []byte, a net.Addr) (int, error) {
	l.log("WriteTo...")
	n, err := l.transport.WriteTo(p, a)
	l.log("WriteTo(%x, %s) --> %d, %s", p, a, n, err)
	return n, err
}

func (l *tracingTransport) SetDeadline(t time.Time) error {
	l.log("setting deadline to %s", t)
	return l.transport.SetDeadline(t)
}

func (l *tracingTransport) SetReadDeadline(t time.Time) error {
	l.log("setting read deadline to %s", t)
	return l.transport.SetReadDeadline(t)
}

func (l *tracingTransport) SetWriteDeadline(t time.Time) error {
	l.log("setting write deadline to %s", t)
	return l.transport.SetWriteDeadline(t)
}

func (l *tracingTransport) SetReadBuffer(n int) error {
	l.log("setting read buffer to %d", n)
	return l.transport.SetReadBuffer(n)
}

func (l *tracingTransport) SyscallConn() (syscall.RawConn, error) {
	l.log("getting syscallconn")
	return l.transport.SyscallConn()
}

// Not traced

func (l *tracingTransport) Addr() net.Addr      { return l.transport.Addr() }
func (l *tracingTransport) LocalAddr() net.Addr { return l.transport.LocalAddr() }
