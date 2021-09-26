package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/soheilhy/cmux"
	"github.com/vsekhar/peers/internal/last"
)

const tagAck = "pok"
const packetBufferSize = 1024

func matchTag(tag string) func(r io.Reader) bool {
	return func(r io.Reader) bool {
		br := bufio.NewReader(io.LimitReader(r, int64(len(tag))))
		l, part, err := br.ReadLine()
		if err != nil || part {
			return false
		}
		return bytes.Equal(l, []byte(tag))
	}
}

var ackMatcher = matchTag(tagAck)

type packet struct {
	payload []byte
	addr    net.Addr
}
type taggedTransport struct {
	Interface
	ctx          context.Context
	cancel       func()
	muxListener  net.Listener
	tag          string
	matcher      func(r io.Reader) bool
	latestPacket *last.Buffer // *packet
}

func (t *taggedTransport) Close() error {
	e1 := t.muxListener.Close()
	t.cancel()
	e2 := t.Interface.Close()
	if e1 != nil || e2 != nil {
		return fmt.Errorf("tagged: mux %w, transport %v", e1, e2)
	}
	return nil
}

func (t *taggedTransport) Dial(network, address string) (net.Conn, error) {
	return t.DialContext(context.Background(), network, address)
}

func (t *taggedTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := t.Interface.DialContext(ctx, network, address)
	if err != nil {
		return c, err
	}
	deadline, _ := ctx.Deadline()
	if err = c.SetDeadline(deadline); err != nil {
		return c, err
	}
	n, err := c.Write([]byte(t.tag))
	if err != nil {
		return c, err
	}
	if n != len(t.tag) {
		return c, io.ErrShortWrite
	}
	if !ackMatcher(c) {
		return c, fmt.Errorf("tag not matched")
	}
	return c, nil
}

func (t *taggedTransport) Accept() (net.Conn, error) {
	c, err := t.muxListener.Accept()
	if err != nil {
		return c, err
	}

	// cmux peaks for the tag but doesn't consume it, so we consume it here.
	if !t.matcher(c) {
		return c, fmt.Errorf("peers: tagged transport internal error: tag does not match")
	}
	n, err := c.Write([]byte(tagAck))
	if err != nil {
		return c, err
	}
	if n != len(tagAck) {
		return c, io.ErrShortWrite
	}

	return c, nil
}

func (t *taggedTransport) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	packet := t.latestPacket.LoadOrWait().(*packet)
	n = copy(p, packet.payload)
	return n, packet.addr, nil
}

func (t *taggedTransport) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	var ar [packetBufferSize]byte
	buf := ar[:]
	i := binary.PutUvarint(buf, uint64(len([]byte(t.tag))))
	i += copy(buf[i:], []byte(t.tag))
	n = copy(buf[i:], p)
	j, err := t.Interface.WriteTo(buf[:i+n], addr)
	if err != nil {
		return n, err
	}
	if n != len(p) && j != (i+n) {
		err = io.ErrShortWrite
	}
	return n, err
}

func Tagged(transport Interface, logger *log.Logger, tags ...string) map[string]Interface {
	r := make(map[string]*taggedTransport, len(tags))
	mux := cmux.New(transport)
	for _, t := range tags {
		ctx, cancel := context.WithCancel(context.Background())
		matcher := matchTag(t)
		r[t] = &taggedTransport{
			Interface:    transport,
			ctx:          ctx,
			cancel:       cancel,
			muxListener:  mux.Match(matcher),
			tag:          t,
			matcher:      matcher,
			latestPacket: last.NewBuffer(),
		}
	}
	mux.HandleError(func(err error) bool {
		if strings.Contains(err.Error(), "use of closed network connection") {
			return false // stop serving
		}
		if logger != nil {
			logger.Printf("peers: mux err: %v", err)
		}
		return true
	})
	go func() {
		if err := mux.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
			if logger != nil {
				logger.Printf("cmux serve: %v", err)
			}
		}
	}()
	go func() {
		buf := make([]byte, packetBufferSize)
		for {
			totalLength, addr, err := transport.ReadFrom(buf)
			if err != nil && logger != nil {
				logger.Printf("peers: packet read: %v", err)
				continue
			}
			s, n := binary.Uvarint(buf)
			if n <= 0 && logger != nil {
				logger.Printf("peers: bad tag size")
				continue
			}
			if t, ok := r[string(buf[n:n+int(s)])]; ok {
				t.latestPacket.Store(&packet{payload: buf[n+int(s) : totalLength], addr: addr})
			}
		}
	}()
	ifaces := make(map[string]Interface)
	for t, ts := range r {
		ifaces[t] = ts
	}
	return ifaces
}
