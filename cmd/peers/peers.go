package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/vsekhar/peers"
	"github.com/vsekhar/peers/discovery/local"
	"github.com/vsekhar/peers/transport"
)

var discoveryType = flag.String("discovery", "", "discovery type: local or none (default)")

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cfg := peers.Config{
		NodeName: fmt.Sprintf("%d", rand.Intn(512)),
	}
	log.Printf("node name: %s", cfg.NodeName)
	transport, err := transport.System(":0")
	if err != nil {
		log.Fatal(err)
	}
	cfg.Transport = transport
	log.Printf("listening on %s", transport.LocalAddr().String())
	switch *discoveryType {
	case "local":
		path := local.GetBinarySpecificPath()
		log.Printf("discovery path: %s", path)
		discoverer, err := local.New(ctx, path, transport.Addr().String(), log.Default())
		if err != nil {
			log.Fatal(err)
		}
		cfg.Discoverer = discoverer
	case "none", "":
	default:
		log.Fatalf("discovery must be 'local', 'none', or not specified")
	}

	p, err := peers.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
			log.Printf("members: %v", p.Members())
		}
	}
}
