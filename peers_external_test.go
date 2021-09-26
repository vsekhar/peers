package peers_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/vsekhar/peers"
	"github.com/vsekhar/peers/discovery/local"
	"github.com/vsekhar/peers/internal/testlog"
	"github.com/vsekhar/peers/internal/testtls"
	"github.com/vsekhar/peers/transport"
)

const (
	peerNetTag = "prn"
	rpcNetTag  = "prc"
)

func goForEach(ps []*peers.Peers, f func(p *peers.Peers, i int)) {
	wg := sync.WaitGroup{}
	wg.Add(len(ps))
	for i := range ps {
		go func(i int) {
			defer wg.Done()
			f(ps[i], i)
		}(i)
	}
	wg.Wait()
}

type testPeers struct {
	peers        []*peers.Peers
	rpcListeners []transport.Interface
}

func makeCluster(ctx context.Context, t *testing.T, n int, logger *log.Logger) *testPeers {
	r := &testPeers{
		peers:        make([]*peers.Peers, n),
		rpcListeners: make([]transport.Interface, n),
	}

	goForEach(r.peers, func(p *peers.Peers, i int) {
		sysTrans, err := transport.System(":0")
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			<-ctx.Done()
			if err := sysTrans.Close(); err != nil && logger != nil {
				logger.Printf("error closing listener %v", err)
			}
		}()
		tcfg := testtls.Config()
		tcfg.ServerName = fmt.Sprintf("peer%d", i)
		tlsTrans := transport.TLSWithInsecureUDP(sysTrans, tcfg)
		tagged := transport.Tagged(tlsTrans, logger, peerNetTag, rpcNetTag)
		discoverer, err := local.New(ctx, sysTrans.Addr().String(), logger)
		if err != nil {
			t.Fatal(err)
		}
		cfg := peers.Config{
			NodeName:   tcfg.ServerName,
			Transport:  tagged[peerNetTag],
			Logger:     logger,
			Discoverer: discoverer,
		}
		r.peers[i], err = peers.New(ctx, cfg)
		r.rpcListeners[i] = tagged[rpcNetTag]
		if err != nil {
			t.Fatal(err)
		}
	})

	return r
}

func TestPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	const numPeers = 5
	logger := testlog.New()
	ps := makeCluster(ctx, t, numPeers, logger.Std())

	time.Sleep(500 * time.Millisecond) // let peers gossip

	goForEach(ps.peers, func(p *peers.Peers, i int) {
		if p.NumPeers() != numPeers {
			t.Errorf("peer %d has %d peers, expected %d peers", i, p.NumPeers(), numPeers)
			t.Errorf("peers of %v: %v", p.LocalAddr(), p.Members())
		}
	})

	// TODO test RPC

	logger.ErrorIfEmpty(t)
	logger.ErrorIfContains(t,
		"ERR",
		"ERROR",
		"error",
		"WARN")

	goForEach(ps.peers, func(p *peers.Peers, _ int) {
		p.Shutdown()
	})
	cancel()

	// Still flaky, data corruption errors around the time of leaving
	logger.ErrorIfEmpty(t)
	logger.ErrorIfContains(t,
		"ERR",
		"ERROR",
		"error")
	// There's usually at least one invalid UDP packet, probably due to MTU
	// discovery.
	logger.ErrorIfContainsMoreThan(t, "[WARN] memberlist: Got invalid checksum for UDP packet", 1)
}
