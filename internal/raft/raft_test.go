package raft_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/vsekhar/peers/internal/raft"
	"github.com/vsekhar/peers/transport"
)

func makeRaft(ctx context.Context) (*hraft.Raft, transport.Interface, error) {
	s, err := transport.System(":0")
	if err != nil {
		return nil, nil, err
	}
	r, err := raft.New(ctx, s.LocalAddr().String(), s)
	if err != nil {
		return nil, nil, err
	}
	return r, s, nil
}

func makeCluster(ctx context.Context, n int) ([]*hraft.Raft, []transport.Interface, error) {
	rafts := make([]*hraft.Raft, n)
	transports := make([]transport.Interface, n)
	for i := range rafts {
		var err error
		rafts[i], transports[i], err = makeRaft(ctx)
		if err != nil {
			return nil, nil, err
		}
	}
	return rafts, transports, nil
}

func TestCluster(t *testing.T) {
	const clusterSize = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rafts, transports, err := makeCluster(ctx, clusterSize)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapAddr := transports[0].LocalAddr().String()
	cfg := hraft.Configuration{
		Servers: []hraft.Server{{
			Suffrage: hraft.Voter,
			ID:       hraft.ServerID(bootstrapAddr),
			Address:  hraft.ServerAddress(bootstrapAddr),
		}},
	}

	// Bootstrap and connect up nodes
	if err := rafts[0].BootstrapCluster(cfg).Error(); err != nil {
		t.Fatal(err)
	}
	// Need to wait for it to make itself the leader, otherwise AddVoter below
	// will fail.
	for !<-rafts[0].LeaderCh() {
	}
	for i := 1; i < len(transports); i++ {
		addr := transports[i].LocalAddr().String()
		f := rafts[0].AddVoter(hraft.ServerID(addr), hraft.ServerAddress(addr), rafts[0].GetConfiguration().Index(), 0)
		if f.Error() != nil {
			t.Errorf("Adding voter %s to %s: %s", addr, transports[0].LocalAddr().String(), f.Error())
		}
	}

	time.Sleep(50 * time.Millisecond)

	// Check that we have exactly one leader and are fully connected
	leaders := 0
	for i, r := range rafts {
		stats := r.Stats()
		t.Logf("%s: %s peers", r.String(), stats["num_peers"])
		numPeers, err := strconv.Atoi(stats["num_peers"])
		if err != nil {
			t.Errorf("parsing num_peers: %v", err)
		}
		if numPeers != clusterSize-1 {
			t.Errorf("%s: num_peers %d, expected %d", r.String(), numPeers, clusterSize-1)
		}
		if r.Leader() == hraft.ServerAddress(transports[i].LocalAddr().String()) {
			leaders++
		}
	}
	if leaders < 1 {
		t.Error("No node thinks it's the leader")
	}
	if leaders > 1 {
		t.Errorf("Multiple leaders: %d", leaders)
	}
}
