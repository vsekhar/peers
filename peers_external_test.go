package peers_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/vsekhar/peers"
	"github.com/vsekhar/peers/internal/testtls"
)

var tlsConfig *tls.Config

func init() {
	tlsConfig = testtls.GenerateTLSConfig()
}

// TODO: test p.Accept and p.DialNext

func TestPeers(t *testing.T) {
	lbuf := &bytes.Buffer{}
	logger := log.New(lbuf, "", 0)
	cfg := peers.Config{
		TLSConfig: tlsConfig,
		Logger:    logger,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cfg.NodeName = "1"
	p1, err := peers.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("p1 listening at: %s", p1.LocalAddr())
	cfg.NodeName = "2"
	p2, err := peers.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("p2 listening at: %s", p2.LocalAddr())
	a := p1.LocalAddr()
	n, err := p2.Join([]string{a})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("failed to join")
	}
	// Provoke some UDP traffic
	time.Sleep(5 * time.Second)

	p2.Shutdown()
	p1.Shutdown()
	cancel()

	logs := lbuf.String()
	if len(logs) == 0 {
		t.Error("no logs")
	}
	if strings.Contains(logs, "ERR") || strings.Contains(logs, "ERROR") {
		t.Error(logs)
	}
}
