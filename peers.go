// Package peers provides peer discovery and services.
package peers // import "github.com/vsekhar/peers"

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/vsekhar/peers/internal/singlego"
)

// TODO: package peerrpc: open a general grpc server and manage a pool of
// general grpc channels (effectively connections). User can wrap these in the
// service stub.
//   Dial: use dial options WithContextDialer and WithInsecure (TLS below)
//   Server: start on a listener and register service

type Network interface {
	net.Listener
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

const maxPacketLength = 1024
const packetBufferLength = 100

const useOfClosedErrString = "use of closed network connection"

// isClosed returns true if err represents a "use of closed connection" i/o
// error.
//
// isClosed can be used to detect when a listen port has been closed normally
// and a listening loop should terminate, vs. some other error.
func isClosed(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if opErr.Err.Error() == useOfClosedErrString {
			return true
		}
	}
	if err.Error() == "server closed" {
		return true
	}
	return false
}

// Config provides configuration parameters for a Host.
type Config struct {
	NodeName   string
	Network    Network
	PacketConn net.PacketConn
	Logger     *log.Logger

	// MemberNotify is called from its own goroutine when membership changes
	// occur. Only one call of MemberNotify will be active at a time, so a
	// previous call must return for subsequent calls to be made.
	//
	// MemberNotify will typically call Members to get the current list of
	// members.
	MemberNotify func()
}

// Peers is the primary object of a peer.
type Peers struct {
	ctx          context.Context
	cancel       func()
	config       Config
	transport    memberlist.Transport
	serf         *serf.Serf
	memberNotify singlego.Trigger

	addr net.Addr
}

func New(pctx context.Context, cfg Config) (*Peers, error) {
	ctx, cancel := context.WithCancel(pctx)
	scfg := serf.DefaultConfig()
	scfg.NodeName = cfg.NodeName
	scfg.Logger = cfg.Logger
	scfg.MemberlistConfig = memberlist.DefaultLANConfig()
	var err error
	scfg.MemberlistConfig.Transport, err = newTransport(ctx, cfg.Network, cfg.PacketConn)
	if err != nil {
		cancel()
		return nil, err
	}
	scfg.MemberlistConfig.Logger = cfg.Logger

	ech := make(chan serf.Event)
	scfg.EventCh = ech
	srf, err := serf.Create(scfg)
	if err != nil {
		cancel()
		return nil, err
	}

	p := &Peers{
		ctx:          ctx,
		cancel:       cancel,
		config:       cfg,
		transport:    scfg.MemberlistConfig.Transport,
		serf:         srf,
		memberNotify: singlego.New(ctx, cfg.MemberNotify),
		addr:         cfg.Network.Addr(),
	}

	// Listen for membership changes and trigger update of maglevhash
	go func() {
		for {
			select {
			case e := <-ech:
				switch e.(type) {
				case serf.MemberEvent:
					p.memberNotify.Notify()
				case serf.UserEvent: // unused
				default:
					panic("unknown event type")
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return p, nil
}

func (p *Peers) LocalAddr() string {
	return p.addr.String()
}

func (p *Peers) Join(nodes []string) (int, error) {
	// Everything is eventually consistent, so we can't guarantee that messages
	// will be delivered. Therefore don't bother trying to deliver old messages,
	// (the second argument below is ignoreOld).
	return p.serf.Join(nodes, true)
}

func (p *Peers) NumPeers() int {
	return p.serf.NumNodes()
}

func (p *Peers) Members() []string {
	r := make([]string, 0, p.NumPeers())
	for _, p := range p.serf.Members() {
		r = append(r, fmt.Sprintf("%s:%d", p.Addr.String(), p.Port))
	}
	return r
}

func (p *Peers) Shutdown() error {
	if err := p.serf.Leave(); err != nil {
		return err
	}
	if err := p.serf.Shutdown(); err != nil {
		return err
	}
	p.cancel()
	return nil
}

var _ memberlist.NodeAwareTransport = &peerTransport{}

// Implements memberlist.Transport
type peerTransport struct {
	ctx     context.Context
	cancel  func()
	network Network
	sch     chan net.Conn
	pconn   net.PacketConn
	pch     chan *memberlist.Packet
}

func newTransport(parentCtx context.Context, n Network, pl net.PacketConn) (*peerTransport, error) {
	ctx, cancel := context.WithCancel(parentCtx)
	r := &peerTransport{
		ctx:     ctx,
		cancel:  cancel,
		network: n,
		sch:     make(chan net.Conn),
		pconn:   pl,
		pch:     make(chan *memberlist.Packet, packetBufferLength),
	}

	if n.Addr().String() != pl.LocalAddr().String() {
		return nil, fmt.Errorf("address mismatch: %s and %s", n.Addr().String(), pl.LocalAddr().String())
	}

	// Listen for and push new streams
	go func() {
		for {
			c, err := n.Accept()
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
	return p.network.DialContext(ctx, "tcp", addr)
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
