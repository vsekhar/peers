package local

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/vsekhar/peers/discovery"
)

const (
	// https://www.iana.org/assignments/multicast-addresses/multicast-addresses.xhtml
	multicastGroupAddr = "224.0.0.151:1803"
	cacheTimeout       = 30 * time.Second
)

func findLoopback() *net.Interface {
	is, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for _, i := range is {
		if (i.Flags & net.FlagLoopback) > 0 {
			return &i
		}
	}
	panic("no loopback interface found")
}

var _ discovery.Interface = &udpMulticastDiscoverer{}

type udpMulticastDiscoverer struct {
	mu     sync.Mutex
	me     string
	uaddr  *net.UDPAddr
	c      *net.UDPConn
	cache  map[string]time.Time
	logger *log.Logger
}

func New(ctx context.Context, me string, logger *log.Logger) (discovery.Interface, error) {
	uaddr, err := net.ResolveUDPAddr("udp", multicastGroupAddr)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenMulticastUDP("udp", findLoopback(), uaddr)
	if err != nil {
		return nil, err
	}
	r := &udpMulticastDiscoverer{
		me:    me,
		uaddr: uaddr,
		c:     c,
		cache: make(map[string]time.Time),
	}
	go func() {
		buf := make([]byte, 1024)
		for {
			n, _, err := c.ReadFrom(buf)
			if err != nil && logger != nil {
				logger.Printf("peers: error reading multicast udp packet: %v", err)
				continue
			}
			member := string(buf[:n])
			r.mu.Lock()
			r.cache[member] = time.Now()
			r.mu.Unlock()
		}
	}()
	return r, nil
}

func (u *udpMulticastDiscoverer) Discover(_ context.Context) []string {
	go func() {
		n, err := u.c.WriteTo([]byte(u.me), u.uaddr)
		if err != nil && u.logger != nil {
			u.logger.Printf("peers: failed to send discovery message: %v", err)
		}
		if n != len(u.me) && u.logger != nil {
			u.logger.Printf("peers: discovery message short write")
		}
	}()

	u.mu.Lock()
	defer u.mu.Unlock()
	start := time.Now()
	r := make([]string, 0, len(u.cache))
	for m, t := range u.cache {
		if start.Sub(t) > cacheTimeout {
			delete(u.cache, m)
			continue
		}
		r = append(r, m)
	}
	return r
}
