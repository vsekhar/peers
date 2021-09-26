// Package peers provides peer discovery and services.
package peers // import "github.com/vsekhar/peers"

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/vsekhar/peers/discovery"
	"github.com/vsekhar/peers/internal/singlego"
	"github.com/vsekhar/peers/transport"
)

const maxPacketLength = 1024
const packetBufferLength = 100

// Config provides configuration parameters for Peers.
type Config struct {
	NodeName  string
	Transport transport.Interface
	Logger    *log.Logger

	// MemberNotify is called from its own goroutine when membership changes
	// occur. Only one call of MemberNotify will be active at a time, so a
	// previous call must return for subsequent calls to be made.
	//
	// MemberNotify will typically call Members to get the current list of
	// members.
	MemberNotify func()

	// Discoverer.Discover is called periodically to obtain out-of-band member
	// discovery. This is useful for connecting new instances to a group of
	// peers.
	Discoverer discovery.Interface
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
	scfg.MemberlistConfig.Transport, err = newSerfTransport(ctx, cfg.Transport)
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

	// Discover members
	if cfg.Discoverer != nil {
		go func() {
			cache := make(map[string]struct{})
			runsWithoutNew := 0
			interval := time.Duration(0)
			for {
				select {
				case <-time.After(interval):
				case <-srf.ShutdownCh():
					return
				case <-ctx.Done():
					return
				}
				start := time.Now()
				mems := cfg.Discoverer.Discover()
				var newMems []string
				newCache := make(map[string]struct{}, len(cache))
				for _, m := range mems {
					if _, ok := cache[m]; !ok {
						newMems = append(newMems, m)
					}
					newCache[m] = struct{}{}
				}
				cache = newCache

				if len(newMems) > 0 {
					if srf.State() != serf.SerfAlive {
						return
					}
					_, err := srf.Join(newMems, true)
					if err != nil && cfg.Logger != nil {
						cfg.Logger.Printf("peers: join error: %v", err)
					}
					runsWithoutNew = 0
				} else {
					runsWithoutNew++
					if runsWithoutNew > 5 {
						runsWithoutNew = 5
					}
				}

				interval = (1<<runsWithoutNew)*time.Second - time.Since(start)
				if p.NumPeers() == 0 {
					interval = 1 * time.Second
				}
			}
		}()
	}

	// Listen for membership changes
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
	p.memberNotify.Close()
	return nil
}
