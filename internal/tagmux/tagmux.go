// Package tagmux provudes a connection multiplexer using client-defined string
// tags.
package tagmux

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

var encoding = binary.LittleEndian

// Dial sequence: CONNECT, 4-byte LE uint16 tag length, [tag]
// Accept sequence: ACCEPT, (read tag length and tag), {OK, NL}
//  - if OK, socket open, any other error and socket closes

const tagLengthFieldSize = 4 // uint16
var okMsg = []byte("OK")
var noListenerMsg = []byte("NL")

type Mux struct {
	listener net.Listener
	network  string
	address  string

	m      map[string]chan net.Conn
	errs   chan error
	mu     sync.Mutex
	closed chan struct{}
}

func (m *Mux) Close() error {
	m.listener.Close() // terminates listening goroutine
	close(m.closed)
	select {
	case err := <-m.errs:
		return err
	default:
		return nil
	}
}

func New(network, address string) (*Mux, error) {
	l, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	m := &Mux{
		listener: l,
		network:  network,
		address:  address,
		m:        make(map[string]chan net.Conn),
		errs:     make(chan error, 10),
		closed:   make(chan struct{}),
	}

	nonBlockErr := func(err error) {
		select {
		case m.errs <- err:
		default:
		}
	}

	// Accept connections, fetch their tags and try to route them
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok {
					if opErr.Err.Error() == "use of closed network connection" {
						return
					}
				}
				nonBlockErr(err)
			} else {
				go func() {
					buf := make([]byte, tagLengthFieldSize)
					n, err := c.Read(buf)
					if err != nil {
						nonBlockErr(err)
						return
					}
					if n != tagLengthFieldSize {
						nonBlockErr(fmt.Errorf("short read when reading tag length"))
						c.Close()
						return
					}
					tagLength := encoding.Uint16(buf)
					tag := make([]byte, tagLength)
					n, err = c.Read(tag)
					if err != nil {
						nonBlockErr(err)
					}
					if n != int(tagLength) {
						nonBlockErr(fmt.Errorf("short read when reading tag"))
					}
					m.mu.Lock()
					ch, ok := m.m[string(tag)]
					m.mu.Unlock()
					if ok {
						_, err := c.Write(okMsg)
						if err != nil {
							nonBlockErr(err)
							c.Close()
							return
						}
						select {
						case ch <- c:
						case <-m.closed:
							return
						}
					} else {
						_, err := c.Write(noListenerMsg)
						if err != nil {
							nonBlockErr(err)
						}
						c.Close()
						return
					}
				}()
			}
		}
	}()

	return m, nil
}

type Listener struct {
	mux *Mux
	tag string
	ch  chan net.Conn
}

func (m *Mux) Listen(tag string) (net.Listener, error) {
	// Consume any pending errors
	select {
	case err := <-m.errs:
		return nil, err
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.m[tag]; ok {
		return nil, &net.OpError{
			Op:     "listen",
			Net:    m.network,
			Source: nil,
			Addr:   m.listener.Addr(),
			Err:    fmt.Errorf("tag in use"),
		}
	} else {
		m.m[tag] = make(chan net.Conn)
	}
	return &Listener{
		mux: m,
		tag: tag,
		ch:  m.m[tag],
	}, nil
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case <-l.mux.closed:
	case c := <-l.ch:
		return c, nil
	}
	return nil, &net.OpError{
		Op:     "listen",
		Net:    l.Addr().Network(),
		Source: nil,
		Addr:   l.Addr(),
		Err:    fmt.Errorf("use of closed network connection"),
	}
}

func (l *Listener) Close() error {
	l.mux.mu.Lock()
	delete(l.mux.m, l.tag)
	l.mux.mu.Unlock()
	return nil
}

func (l *Listener) Addr() net.Addr {
	return l.mux.listener.Addr()
}

func Dial(network, address, tag string) (net.Conn, error) {
	c, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	tagBytes := []byte(tag)
	tagLengthBytes := make([]byte, tagLengthFieldSize)
	encoding.PutUint16(tagLengthBytes, uint16(len(tagBytes)))
	n, err := c.Write(tagLengthBytes)
	if err != nil {
		c.Close()
		return nil, err
	}
	if n < tagLengthFieldSize {
		c.Close()
		return nil, fmt.Errorf("short write sending tag length")
	}
	n, err = c.Write(tagBytes)
	if err != nil {
		c.Close()
		return nil, err
	}
	if n < len(tagBytes) {
		c.Close()
		return nil, fmt.Errorf("short write sending tag")
	}
	buf := make([]byte, len(okMsg))
	_, err = c.Read(buf)
	if err != nil {
		c.Close()
		return nil, err
	}
	if bytes.Equal(buf, okMsg) {
		return c, nil
	}
	if bytes.Equal(buf, noListenerMsg) {
		c.Close()
		return nil, &net.OpError{
			Op:     "dial",
			Net:    network,
			Source: c.LocalAddr(),
			Addr:   c.RemoteAddr(),
			Err:    fmt.Errorf("failed to connect"),
		}
	}
	c.Close()
	return nil, fmt.Errorf("tagmux: bad response %v", buf)
}
