package raft

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/vsekhar/peers/transport"
)

type raftStream struct {
	transport.Interface
}

var _ raft.StreamLayer = (*raftStream)(nil)

func (r *raftStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return r.DialContext(ctx, "tcp", string(address))
}

type raftFSM struct{}

var _ raft.FSM = (*raftFSM)(nil)

func (r *raftFSM) Apply(l *raft.Log) interface{} {
	panic("unimplemented")
}

func (r *raftFSM) Snapshot() (raft.FSMSnapshot, error) {
	panic("unimplemented")
}

func (r *raftFSM) Restore(reader io.ReadCloser) error {
	panic("unimplemented")
}

func New(ctx context.Context, id string, transport transport.Interface) (*raft.Raft, error) {
	// TODO interface to make this persistant during restarts? Or do we just
	// toss the state?
	bdir, err := ioutil.TempDir("", "peersbolddbtest")
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		os.RemoveAll(bdir)
	}()

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(id)
	logsFilename := filepath.Join(bdir, "logs.dat")
	stableFilename := filepath.Join(bdir, "stable.dat")
	ldb, err := boltdb.NewBoltStore(logsFilename)
	if err != nil {
		return nil, err
	}
	sdb, err := boltdb.NewBoltStore(stableFilename)
	if err != nil {
		return nil, err
	}
	fss, err := raft.NewFileSnapshotStore(bdir, 3, os.Stderr)
	if err != nil {
		return nil, err
	}

	tcfg := &raft.NetworkTransportConfig{
		ServerAddressProvider: nil,
		Logger:                nil,
		Stream:                &raftStream{Interface: transport},
		MaxPool:               100,
		Timeout:               10 * time.Second,
	}
	raftTransport := raft.NewNetworkTransportWithConfig(tcfg)
	fsm := &raftFSM{}
	return raft.NewRaft(c, fsm, ldb, sdb, fss, raftTransport)
}
