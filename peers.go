// Package peers provides peer discovery and services.
package peers // import "github.com/vsekhar/peers"

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	reuse "github.com/libp2p/go-reuseport"
)

const (
	serfNetTag = "psn"
	rpcNetTag  = "rpc"
)

func init() {
	if !reuse.Available() {
		panic("peers: OS must support SO_REUSEADDR and SO_REUSEPORT (darwin, linux, windows)")
	}
}

func matchTag(tag string) func(r io.Reader) bool {
	return func(r io.Reader) bool {
		br := bufio.NewReader(io.LimitReader(r, int64(len(tag))))
		l, part, err := br.ReadLine()
		if err != nil || part {
			return false
		}
		return bytes.Equal(l, []byte(tag))
	}
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
	// NodeName is the name of the host. It must be unique within the cluster.
	//
	// If an empty name is provided, a randomly-generated one will be used.
	NodeName string

	// BindAddress provides the local bind point in "host:port" format. If none
	// is provided, peers will bind to a dynamically-assigned port.
	BindAddress string
	TLSConfig   tls.Config
	Logger      *log.Logger
}

// Peers is the primary object of a peer.
type Peers struct {
	ctx    context.Context
	cancel func()
	config Config
	mux    cmux.CMux
	serf   *serf.Serf

	addr net.Addr
}

func New(pctx context.Context, cfg Config) (*Peers, error) {
	ctx, cancel := context.WithCancel(pctx)
	if cfg.NodeName == "" {
		if cfg.TLSConfig.ServerName == "" {
			cfg.NodeName = uuid.New().String()
		} else {
			cfg.NodeName = cfg.TLSConfig.ServerName
		}
	}
	cfg.TLSConfig.ServerName = cfg.NodeName

	l, err := reuse.Listen("tcp", cfg.BindAddress)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		if err := l.Close(); err != nil {
			log.Printf("peers: error closing listeniner: %v", err)
		}
	}()

	cm := cmux.New(l)
	serfL := cm.Match(matchTag(serfNetTag))
	cm.HandleError(func(err error) bool {
		if strings.Contains(err.Error(), "use of closed network connection") {
			return false
		}
		log.Printf("peers: mux err: %v", err)
		return true
	})

	var paddr string
	if cfg.BindAddress == "" {
		paddr = l.Addr().String()
	}
	pl, err := reuse.ListenPacket("udp", paddr)
	if err != nil {
		return nil, err
	}
	scfg := serf.DefaultConfig()
	scfg.NodeName = cfg.NodeName
	scfg.Logger = cfg.Logger
	scfg.MemberlistConfig = memberlist.DefaultLANConfig()
	scfg.MemberlistConfig.Transport, err = NewTransport(ctx, serfL, pl)
	if err != nil {
		return nil, err
	}
	scfg.MemberlistConfig.Logger = cfg.Logger
	srf, err := serf.Create(scfg)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := cm.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
			log.Printf("cmux serve: %v", err)
		}
	}()

	return &Peers{
		ctx:    ctx,
		cancel: cancel,
		config: cfg,
		mux:    cm,
		serf:   srf,
		addr:   l.Addr(),
	}, nil
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

// TODO: encryption
// TODO: client can get member information
// TODO: client can monitor for joins/leaves
// TODO: client can RPC
// TODO: client can route requests based on string using maglevhash

var _ memberlist.NodeAwareTransport = &peerTransport{}

// Implements memberlist.Transport
type peerTransport struct {
	ctx      context.Context
	cancel   func()
	listener net.Listener
	sch      chan net.Conn
	pconn    net.PacketConn
	pch      chan *memberlist.Packet
}

func NewTransport(parentCtx context.Context, l net.Listener, pl net.PacketConn) (*peerTransport, error) {
	ctx, cancel := context.WithCancel(parentCtx)
	r := &peerTransport{
		ctx:      ctx,
		cancel:   cancel,
		listener: l,
		sch:      make(chan net.Conn),
		pconn:    pl,
		pch:      make(chan *memberlist.Packet, packetBufferLength),
	}

	if l.Addr().String() != pl.LocalAddr().String() {
		return nil, fmt.Errorf("address mismatch: %s and %s", l.Addr().String(), pl.LocalAddr().String())
	}

	// Listen for and push new streams
	go func() {
		for {
			c, err := r.listener.Accept()
			if err != nil {
				if isClosed(err) {
					close(r.sch)
					return
				}
				// TODO: log?
				continue
			}

			// Check (and consume) protocol header (cmux already checked for it
			// but doesn't comsume it)
			if !matchTag(serfNetTag)(c) {
				panic("peers: tag not found")
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
	deadline := time.Now().Add(timeout)
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if err := c.SetWriteDeadline(deadline); err != nil {
		c.Close()
		return nil, err
	}
	n, err := c.Write([]byte(serfNetTag))
	if err != nil {
		c.Close()
		return nil, err
	}
	if n != len(serfNetTag) {
		c.Close()
		return nil, io.ErrShortWrite
	}
	if err := c.SetWriteDeadline(time.Time{}); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
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
