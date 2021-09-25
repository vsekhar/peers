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

	"github.com/vsekhar/peers"
	"github.com/vsekhar/peers/internal/syncbuf"
	"github.com/vsekhar/peers/internal/testtls"
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

func makeCluster(ctx context.Context, t *testing.T, n int, logger *log.Logger) []*peers.Peers {
	ps := make([]*peers.Peers, n)

	// create
	goForEach(ps, func(p *peers.Peers, i int) {
		cfg := peers.Config{
			NodeName:  fmt.Sprintf("node%d", i),
			TLSConfig: tlsConfig,
			Logger:    logger,
		}
		var err error
		ps[i], err = peers.New(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
	})

	// connect
	goForEach(ps, func(p *peers.Peers, i int) {
		n, err := p.Join([]string{ps[(i+1)%len(ps)].LocalAddr()})
		if err != nil {
			t.Error(err)
		}
		if n != 1 {
			t.Error("failed to join")
		}

	})

	return ps
}

func TestPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	const numPeers = 5
	lbuf := new(syncbuf.Syncbuf)
	logger := log.New(lbuf, "", log.LstdFlags|log.Lshortfile)
	ps := makeCluster(ctx, t, numPeers, logger)

	time.Sleep(500 * time.Millisecond) // let peers gossip

	goForEach(ps, func(p *peers.Peers, i int) {
		if p.NumPeers() != numPeers {
			t.Errorf("peer %d has %d peers, expected %d peers", i, p.NumPeers(), numPeers)
			t.Errorf("peers of %v: %v", p.LocalAddr(), p.Members())
		}
	})

	goForEach(ps, func(p *peers.Peers, _ int) {
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
