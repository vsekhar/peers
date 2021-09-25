package peers

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/memberlist"
)

var _ memberlist.NodeAwareTransport = &peerTransport{}

// Implements memberlist.Transport
type peerTransport struct {
	ctx       context.Context
	cancel    func()
	transport Transport
	sch       chan net.Conn
	pconn     net.PacketConn
	pch       chan *memberlist.Packet
}

func newTransport(parentCtx context.Context, t Transport, pl net.PacketConn) (*peerTransport, error) {
	ctx, cancel := context.WithCancel(parentCtx)
	r := &peerTransport{
		ctx:       ctx,
		cancel:    cancel,
		transport: t,
		sch:       make(chan net.Conn),
		pconn:     pl,
		pch:       make(chan *memberlist.Packet, packetBufferLength),
	}

	if t.Addr().String() != pl.LocalAddr().String() {
		return nil, fmt.Errorf("address mismatch: %s and %s", t.Addr().String(), pl.LocalAddr().String())
	}

	// Listen for and push new streams
	go func() {
		for {
			c, err := t.Accept()
			if err != nil {
				if isClosed(err) {
					close(r.sch)
					return
				}
				// TODO: log?
				continue
			}

			// Get back to listening asap
			// TODO: control how many of these are created?
			go func() {
				select {
				case r.sch <- c:
				case <-r.ctx.Done():
					c.Close()
					return
				}
			}()

		}
	}()

	// Listen for and push new packets
	go func() {
		for {
			buf := make([]byte, maxPacketLength)
			n, addr, err := r.pconn.ReadFrom(buf)
			now := time.Now()
			if err != nil {
				if isClosed(err) {
					close(r.pch)
					return
				}
			}
			p := &memberlist.Packet{
				Buf:       buf[:n],
				From:      addr,
				Timestamp: now,
			}
			select {
			case r.pch <- p:
			case <-r.ctx.Done():
				return
			}
		}
	}()

	return r, nil
}

// FinalAdvertiseAddr is given the user's configured values (which
// might be empty) and returns the desired IP and port to advertise to
// the rest of the cluster.
func (p *peerTransport) FinalAdvertiseAddr(_ string, _ int) (net.IP, int, error) {
	a := p.pconn.LocalAddr()
	addr, portStr, err := net.SplitHostPort(a.String())
	if err != nil {
		return nil, 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, 0, err
	}
	ipAddr := net.ParseIP(addr)
	return ipAddr, port, nil
}

// WriteTo is a packet-oriented interface that fires off the given
// payload to the given address in a connectionless fashion. This should
// return a time stamp that's as close as possible to when the packet
// was transmitted to help make accurate RTT measurements during probes.
//
// This is similar to net.PacketConn, though we didn't want to expose
// that full set of required methods to keep assumptions about the
// underlying plumbing to a minimum. We also treat the address here as a
// string, similar to Dial, so it's network neutral, so this usually is
// in the form of "host:port".
func (p *peerTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	// TODO: cache these
	uaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return time.Time{}, err
	}
	n, err := p.pconn.WriteTo(b, uaddr)
	now := time.Now()
	if err != nil {
		return now, err
	}
	if n != len(b) {
		return now, io.ErrShortWrite
	}
	return now, nil
}

// PacketCh returns a channel that can be read to receive incoming
// packets from other peers. How this is set up for listening is left as
// an exercise for the concrete transport implementations.
func (p *peerTransport) PacketCh() <-chan *memberlist.Packet {
	return p.pch
}

// DialTimeout is used to create a connection that allows us to perform
// two-way communication with a peer. This is generally more expensive
// than packet connections so is used for more infrequent operations
// such as anti-entropy or fallback probes if the packet-oriented probe
// failed.
func (p *peerTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()
	return p.transport.DialContext(ctx, "tcp", addr)
}

// StreamCh returns a channel that can be read to handle incoming stream
// connections from other peers. How this is set up for listening is
// left as an exercise for the concrete transport implementations.
func (p *peerTransport) StreamCh() <-chan net.Conn {
	return p.sch
}

// Shutdown is called when memberlist is shutting down; this gives the
// transport a chance to clean up any listeners.
func (p *peerTransport) Shutdown() error {
	p.cancel()
	return nil
}

func (p *peerTransport) WriteToAddress(b []byte, addr memberlist.Address) (time.Time, error) {
	return p.WriteTo(b, addr.Addr)
}

func (p *peerTransport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	return p.DialTimeout(addr.Addr, timeout)
}