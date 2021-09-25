// Package peers provides peer discovery and services.
package peers // import "github.com/vsekhar/peers"

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/vsekhar/peers/internal/singlego"
)

// TODO: package peerrpc: open a general grpc server and manage a pool of
// general grpc channels (effectively connections). User can wrap these in the
// service stub.
//   Dial: use dial options WithContextDialer and WithInsecure (TLS below)
//   Server: start on a listener and register service

type Transport interface {
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
	Transport  Transport
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
	scfg.MemberlistConfig.Transport, err = newTransport(ctx, cfg.Transport, cfg.PacketConn)
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
		addr:         cfg.Transport.Addr(),
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
