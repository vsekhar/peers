package peers_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	reuse "github.com/libp2p/go-reuseport"
	"github.com/vsekhar/peers"
	"github.com/vsekhar/peers/internal/pmux"
	"github.com/vsekhar/peers/internal/syncbuf"
	"github.com/vsekhar/peers/internal/testtls"
)

const (
	peerNetTag = "prn"
	rpcNetTag  = "prc"
)

var tlsConfig *tls.Config

func init() {
	tlsConfig = testtls.GenerateTLSConfig()
}

// TODO: test p.Accept and p.DialNext

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
	rpcListeners []peers.Network
}

func makeCluster(ctx context.Context, t *testing.T, n int, logger *log.Logger) *testPeers {
	r := &testPeers{
		peers:        make([]*peers.Peers, n),
		rpcListeners: make([]peers.Network, n),
	}

	// create
	goForEach(r.peers, func(p *peers.Peers, i int) {
		rl, err := reuse.Listen("tcp", ":0")
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			<-ctx.Done()
			if err := rl.Close(); err != nil && logger != nil {
				logger.Printf("error closing listener %v", err)
			}
		}()
		tcfg := tlsConfig.Clone()
		tcfg.ServerName = fmt.Sprintf("peer%d", i)
		tl := tls.NewListener(rl, tcfg)
		td := &tls.Dialer{Config: tcfg}
		pm := pmux.New(tl, td, logger, peerNetTag, rpcNetTag)
		pl, err := reuse.ListenPacket("udp", rl.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		cfg := peers.Config{
			NodeName:   tcfg.ServerName,
			Network:    pm[peerNetTag],
			PacketConn: pl,
			Logger:     logger,
		}
		r.peers[i], err = peers.New(ctx, cfg)
		r.rpcListeners[i] = pm[rpcNetTag]
		if err != nil {
			t.Fatal(err)
		}
	})

	// connect
	goForEach(r.peers, func(p *peers.Peers, i int) {
		n, err := p.Join([]string{r.peers[(i+1)%len(r.peers)].LocalAddr()})
		if err != nil {
			t.Error(err)
		}
		if n != 1 {
			t.Error("failed to join")
		}

	})

	return r
}

func TestPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	const numPeers = 5
	lbuf := new(syncbuf.Syncbuf)
	logger := log.New(lbuf, "", log.LstdFlags|log.Lshortfile)
	ps := makeCluster(ctx, t, numPeers, logger)

	time.Sleep(500 * time.Millisecond) // let peers gossip

	goForEach(ps.peers, func(p *peers.Peers, i int) {
		if p.NumPeers() != numPeers {
			t.Errorf("peer %d has %d peers, expected %d peers", i, p.NumPeers(), numPeers)
			t.Errorf("peers of %v: %v", p.LocalAddr(), p.Members())
		}
	})

	goForEach(ps.peers, func(p *peers.Peers, _ int) {
		p.Shutdown()
	})
	cancel()

	lbuf.Close()
	logs := lbuf.String()
	if len(logs) == 0 {
		t.Error("no logs")
	}
	if strings.Contains(logs, "ERR") || strings.Contains(logs, "ERROR") {
		t.Error(logs)
	}
}
