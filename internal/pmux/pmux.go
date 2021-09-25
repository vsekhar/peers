// Package pmux provides peer muxing, so that peers can engage in arbitrary
// communication with each other and other clients and services via a single
// listening port.
package pmux

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

type tagConsumer struct {
	net.Listener
	tag string
}

func (tc *tagConsumer) Accept() (c net.Conn, err error) {
	c, err = tc.Listener.Accept()
	if err != nil {
		return
	}

	// cmux peaks for the tag but doesn't consume it, so we consume it here.
	if !matchTag(tc.tag)(c) {
		return c, fmt.Errorf("pmux: internal error: tag does not match")
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

type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type Network interface {
	net.Listener
	ContextDialer
}

type network struct {
	net.Listener
	ContextDialer
	tag string
}

func (pn network) DialContext(ctx context.Context, network, address string) (c net.Conn, err error) {
	c, err = pn.ContextDialer.DialContext(ctx, network, address)
	if err != nil {
		return
	}
	deadline, _ := ctx.Deadline()
	if err = c.SetDeadline(deadline); err != nil {
		return
	}
	n, err := c.Write([]byte(pn.tag))
	if err != nil {
		return
	}
	if n != len(pn.tag) {
		return c, io.ErrShortWrite
	}
	if !matchTag(tagAck)(c) {
		return c, fmt.Errorf("tag not matched")
	}
	return
}

func New(l net.Listener, c ContextDialer, logger *log.Logger, tags ...string) map[string]Network {
	r := make(map[string]Network)
	cm := cmux.New(l)
	for _, t := range tags {
		r[t] = &network{
			Listener: &tagConsumer{
				Listener: cm.Match(matchTag(t)),
				tag:      t,
			},
			ContextDialer: c,
			tag:           t,
		}
	}
	cm.HandleError(func(err error) bool {
		if strings.Contains(err.Error(), "use of closed network connection") {
			return false
		}
		if logger != nil {
			logger.Printf("peers: mux err: %v", err)
		}
		return true
	})
	go func() {
		if err := cm.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
			if logger != nil {
				logger.Printf("cmux serve: %v", err)
			}
		}
	}()
	return r
}
