package local

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/vsekhar/peers/discovery"
	"github.com/vsekhar/peers/internal/syncbuf"
)

func within(i, target, within int) bool {
	diff := i - target
	if diff < 0 {
		diff = -diff
	}
	if diff <= within {
		return true
	}
	return false
}

func TestLocal(t *testing.T) {
	numDiscoverers := 10

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lbuf := &syncbuf.Syncbuf{}
	logger := log.New(lbuf, "", log.LstdFlags|log.Lshortfile)

	ds := make([]discovery.Interface, numDiscoverers)
	for i := range ds {
		var err error
		ds[i], err = New(ctx, fmt.Sprintf("test%d", i), logger)
		if err != nil {
			t.Fatal(err)
		}
		ds[i].Discover()
	}
	peers := ds[0].Discover()
	if !within(len(peers), numDiscoverers-1, 1) {
		// Some flakiness
		t.Errorf("expected %d peers, got %d", 3, len(peers))
	}
}
