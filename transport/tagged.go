package transport

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/soheilhy/cmux"
)

const tagAck = "pok"

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

type taggedTransport struct {
	ctx     context.Context
	cancel  func()
	l       net.Listener
	d       ContextDialer
	tag     string
	matcher func(r io.Reader) bool
}

func (t *taggedTransport) Addr() net.Addr { return t.l.Addr() }
func (t *taggedTransport) Close() error {
	err := t.l.Close()
	t.cancel()
	return err
}

func (t *taggedTransport) Dial(network, address string) (net.Conn, error) {
	return t.DialContext(context.Background(), network, address)
}

func (t *taggedTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := t.d.DialContext(ctx, network, address)
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
	c, err := t.l.Accept()
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

func Tagged(transport Interface, logger *log.Logger, tags ...string) map[string]Interface {
	r := make(map[string]Interface, len(tags))
	mux := cmux.New(transport)
	for _, t := range tags {
		ctx, cancel := context.WithCancel(context.Background())
		matcher := matchTag(t)
		r[t] = &taggedTransport{
			ctx:     ctx,
			cancel:  cancel,
			d:       transport,
			l:       mux.Match(matcher),
			tag:     t,
			matcher: matcher,
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
	return r
}
