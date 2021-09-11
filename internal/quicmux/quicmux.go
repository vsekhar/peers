// Package quicmux provides a user-space network stack that requires only one
// underlying UDP port in order to accept arbitrary UDP and TCP connections.
//
// A single underlying port is useful for hosts that need to communicate with
// each other through underlying runtimes (e.g. containers) and shared or cloud
// networks where ports are scarce or manually allocated.
//
// Clients and servers must both use quicmux so that the proper negotiation can
// take place. Connecting quicmux clients to "regular" servers or vice versa is
// not supported.
//
// Quic was selected since it supports arbitrarily-sized UDP packets, which
// ensures the encapsulation of quicmux does not produce more fragmentation at
// the application layer, and thus remains transparent to the application.
package quicmux

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	quic "github.com/lucas-clemente/quic-go"
)

// Listener: listens on one quic port, produces arbitrary Network each with
// a given tag. NewNetwork(tag string).... Listener routes new TCP connections
// (streams) or UDP packets (messages) to appropriate Networks based on their
// tags.
//
//   Checking tags: Connect/accept, 4-byte LE uint16 tag length, tag, OK/NL
//    - Close socket on any error

// Network: supports outbound (Dial) and inbound (Listen) connections in the
// expected way, internally augmenting the tag given when the Network was
// created. Supports TCP and UDP

var encoding = binary.LittleEndian

const tagLengthFieldSize = 4 // uint16
const packetConnBufferSize = 8

var okMsg = []byte("OK")
var noListenerMsg = []byte("NL")

const useOfClosedErrString = "use of closed network connection"

// IsClosed returns true if err represents a "use of closed connection" i/o
// error.
//
// IsClosed can be used to detect when a listen port has been closed normally
// and a listening loop should terminate, vs. some other error.
func IsClosed(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if opErr.Err.Error() == useOfClosedErrString {
			return true
		}
	}
	return false
}

func readTag(r io.Reader) (string, error) {
	buf := make([]byte, tagLengthFieldSize)
	n, err := r.Read(buf)
	if err != nil {
		return "", err
	}
	if n != tagLengthFieldSize {
		return "", fmt.Errorf("short read when reading tag length")
	}
	tagLength := encoding.Uint16(buf)
	tag := make([]byte, tagLength)
	n, err = r.Read(tag)
	if err != nil {
		return "", err
	}
	if n != int(tagLength) {
		return "", fmt.Errorf("short read when reading tag")
	}
	return string(tag), nil
}

type streamConn struct {
	quic.Stream
	session quic.Session
}

func (s streamConn) LocalAddr() net.Addr {
	return s.session.LocalAddr()
}

func (s streamConn) RemoteAddr() net.Addr {
	return s.session.RemoteAddr()
}

type streamEntry struct {
	n  int
	ch chan streamConn
}

type packet struct {
	addr    net.Addr
	payload []byte
}

type packetEntry struct {
	n  int
	ch chan packet
}

type Mux struct {
	// Underlying listener for incoming QUIC connections from new hosts
	listener quic.Listener
	cfg      *quic.Config
	tcfg     *tls.Config

	// For Networks
	mu            sync.Mutex
	streamEntries map[string]*streamEntry
	packetEntries map[string]*packetEntry

	// For outgoing messages/packets
	sessions map[string]quic.Session

	errs   chan error
	closed chan struct{}
}

func (m *Mux) getOrDialSession(address string) (quic.Session, error) {
	m.mu.Lock()
	session, ok := m.sessions[address]
	m.mu.Unlock()
	if !ok {
		var err error
		// Dial might be slow (name resolution) so don't hold the lock
		session, err = quic.DialAddr(address, m.tcfg, m.cfg)
		if err != nil {
			return nil, err
		}

		// check again and add, use the one present if it exists
		added := false
		m.mu.Lock()
		s, ok := m.sessions[address]
		if !ok {
			m.sessions[address] = session
			added = true
		} else {
			session = s
		}
		m.mu.Unlock()
		if !added {
			session.CloseWithError(quic.ApplicationErrorCode(quic.NoError), "")
		}
	}
	return session, nil
}

func (m *Mux) Close() error {
	m.doClose()
	select {
	case err := <-m.errs:
		return err
	default:
		return nil
	}
}

func (m *Mux) doClose() {
	m.listener.Close()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sl := range m.streamEntries {
		close(sl.ch)
	}
	for _, pl := range m.packetEntries {
		close(pl.ch)
	}
}

func (m *Mux) nonBlockErr(err error) {
	select {
	case m.errs <- err:
	default:
	}
}

func New(ctx context.Context, address string, tcfg *tls.Config) (*Mux, error) {
	cfg := &quic.Config{
		EnableDatagrams: true,
	}
	l, err := quic.ListenAddr(address, tcfg, cfg)
	if err != nil {
		return nil, err
	}
	m := &Mux{
		listener:      l,
		cfg:           cfg,
		tcfg:          tcfg,
		streamEntries: make(map[string]*streamEntry),
		packetEntries: make(map[string]*packetEntry),
		sessions:      make(map[string]quic.Session),
		errs:          make(chan error, 10),
		closed:        make(chan struct{}),
	}

	// A mux launches a set of goroutines to route streams:
	//
	//   1. A listener goroutine accepts new streams and launches a negotiator
	//   2. A negotiator goroutine determines and acknowledges the tag and sends
	//      the stream and the tag to the matcher
	//   3. A matcher looks for a channel in the map to send the stream to. If
	//      none, or if the listener is not immediately ready, the matcher hands
	//      the stream to a closer goroutine and removes and closes the channel
	//      if any.
	//   4. A closer goroutine sends a "no listener" message on the stream and
	//      drops it.
	//
	// Listeners look for (and add if not existing) a channel for their tag. If
	// the channel is closed without getting a stream, Listeners loop.
	//
	// Listener channels are only ever closed if a stream comes in and no one is
	// listening. TODO: refcount listeners and clean up?

	// Handle incoming sessions on this listener
	go func() {
		for {
			session, err := l.Accept(ctx)
			if err != nil {
				if IsClosed(err) {
					// Listener is closed, no more is coming, wrap up
					m.doClose()
					return
				}
				m.nonBlockErr(err)
				continue
			}
			m.mu.Lock()
			m.sessions[session.RemoteAddr().String()] = session
			m.mu.Unlock()

			// Handle incoming streams on this session
			go func() {
				for {
					stream, err := session.AcceptStream(ctx)
					if err != nil {
						if IsClosed(err) {
							// Session is closed, no more is coming, wrap up
							return
						}
						m.nonBlockErr(err)
						continue
					}

					// Handle stream by negotiating tag and attempting to send
					// to a listener
					go func() {
						tag, err := readTag(stream)
						if err != nil {
							m.nonBlockErr(err)
							stream.Close()
							return
						}
						m.mu.Lock()
						sl, ok := m.streamEntries[string(tag)]
						m.mu.Unlock()
						if ok {
							_, err := stream.Write(okMsg)
							if err != nil {
								m.nonBlockErr(err)
								stream.Close()
								return
							}
							select {
							case sl.ch <- streamConn{stream, session}:
							case <-ctx.Done():
							case <-m.closed:
							}
						} else {
							_, err := stream.Write(noListenerMsg)
							if err != nil {
								m.nonBlockErr(err)
							}
							stream.Close()
						}
					}()
				}
			}()

			// Handle incoming messages on this session
			go func() {
				for {
					b, err := session.ReceiveMessage()
					if err != nil {
						if IsClosed(err) {
							// Session is closed, no more is coming, wrap up
							return
						}
						m.nonBlockErr(err)
						continue
					}
					if len(b) < tagLengthFieldSize {
						m.nonBlockErr(fmt.Errorf("short packet"))
						continue
					}
					buf := bytes.NewBuffer(b)
					tag, err := readTag(buf)
					if err != nil {
						m.nonBlockErr(err)
						continue
					}
					m.mu.Lock()
					pl, ok := m.packetEntries[tag]
					m.mu.Unlock()
					if !ok {
						continue // No listener, drop the packet
					}
					p := packet{
						addr:    session.RemoteAddr(),
						payload: b[tagLengthFieldSize+len(tag):],
					}
					select {
					case pl.ch <- p:
					case <-ctx.Done():
						return
					case <-m.closed:
						return
					default:
						// If the listener isn't ready (packetConnBufferSize is
						// full), drop the packet.
					}
				}
			}()
		}
	}()

	return m, nil
}

type Network struct {
	tag    string
	m      *Mux
	closed chan struct{}
}

func (m *Mux) NewNetwork(tag string) *Network {
	n := &Network{
		tag:    tag,
		m:      m,
		closed: make(chan struct{}),
	}
	return n
}

func (n *Network) Tag() string {
	return n.tag
}

func (n *Network) Dial(address string) (net.Conn, error) {
	return n.DialContext(context.Background(), address)
}

func (n *Network) DialContext(ctx context.Context, address string) (net.Conn, error) {
	session, err := n.m.getOrDialSession(address)
	if err != nil {
		return nil, err
	}
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	// Send tag
	tagBytes := []byte(n.tag)
	tagLengthBytes := make([]byte, tagLengthFieldSize)
	encoding.PutUint16(tagLengthBytes, uint16(len(tagBytes)))
	l, err := stream.Write(tagLengthBytes)
	if err != nil {
		return nil, err
	}
	if l < tagLengthFieldSize {
		return nil, fmt.Errorf("short write sending tag length")
	}
	l, err = stream.Write(tagBytes)
	if err != nil {
		return nil, err
	}
	if l < len(tagBytes) {
		return nil, fmt.Errorf("short write sending tag")
	}

	// Wait for OK
	buf := make([]byte, len(okMsg))
	_, err = stream.Read(buf)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(buf, noListenerMsg) {
		return nil, &net.OpError{
			Op:  "dial",
			Err: fmt.Errorf("failed to connect"),
		}
	}
	if bytes.Equal(buf, okMsg) {
		return streamConn{stream, session}, nil
	}
	return nil, fmt.Errorf("quicmux: bad response %v", buf)
}

func (n *Network) LocalAddr() net.Addr {
	return n.m.listener.Addr()
}

func (n *Network) Close() error {
	close(n.closed)
	// TODO: close open sockets?
	return nil
}

type streamListener struct {
	n      *Network
	closed chan struct{}
}

func (nl *streamListener) Addr() net.Addr {
	return nl.n.m.listener.Addr()
}

func (nl *streamListener) Close() error {
	close(nl.closed)
	return nil
}

func (n *Network) Listen() (net.Listener, error) {
	l := &streamListener{
		n:      n,
		closed: make(chan struct{}),
	}
	return l, nil
}

func (l *streamListener) Accept() (net.Conn, error) {
	return l.AcceptContext(context.Background())
}

func (l *streamListener) AcceptContext(ctx context.Context) (net.Conn, error) {
	for {
		// Add ourselves as a listener for new streams
		mux := l.n.m
		mux.mu.Lock()
		if _, ok := mux.streamEntries[l.n.tag]; !ok {
			mux.streamEntries[l.n.tag] = &streamEntry{
				ch: make(chan streamConn),
			}
		}
		mux.streamEntries[l.n.tag].n++
		ch := mux.streamEntries[l.n.tag].ch
		mux.mu.Unlock()

		// Remove when done
		defer func() {
			mux.mu.Lock()
			defer mux.mu.Unlock()
			e, ok := mux.streamEntries[l.n.tag]
			if !ok {
				return
			}
			e.n--
			if e.n < 0 {
				panic("removeStreamEntry underflow")
			}
			if e.n == 0 {
				close(e.ch)
				delete(mux.streamEntries, l.n.tag)
			}
		}()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case sc, ok := <-ch:
			if !ok {
				// listener closed
				return nil, &net.OpError{
					Op:     "listen",
					Net:    l.Addr().Network(),
					Source: nil,
					Addr:   l.Addr(),
					Err:    fmt.Errorf(useOfClosedErrString),
				}
			}
			return sc, nil
		case <-l.closed:
			return nil, &net.OpError{
				Op:     "listen",
				Net:    l.Addr().Network(),
				Source: nil,
				Addr:   l.Addr(),
				Err:    fmt.Errorf(useOfClosedErrString),
			}
		}
	}
}

type packetConn struct {
	n      *Network
	ch     chan packet
	closed chan struct{}
}

func (p *packetConn) Close() error {
	p.n.m.mu.Lock()
	defer p.n.m.mu.Unlock()
	e, ok := p.n.m.packetEntries[p.n.tag]
	if !ok {
		return nil
	}
	e.n--
	if e.n < 0 {
		panic("removePacketEntry underflow")
	}
	if e.n == 0 {
		close(e.ch)
		delete(p.n.m.packetEntries, p.n.tag)
	}
	close(p.closed)
	return nil
}

func (p *packetConn) LocalAddr() net.Addr {
	return p.n.m.listener.Addr()
}

func (p *packetConn) ReadFrom(buf []byte) (n int, addr net.Addr, err error) {
	select {
	case packet, ok := <-p.ch:
		if !ok {
			return 0, nil, &net.OpError{
				Op:     "listen",
				Net:    p.n.m.listener.Addr().Network(),
				Source: nil,
				Addr:   p.n.m.listener.Addr(),
				Err:    fmt.Errorf(useOfClosedErrString),
			}
		}
		n = copy(buf, packet.payload)
		addr = packet.addr
		err = nil
		return
	case <-p.closed:
		return 0, nil, &net.OpError{
			Op:     "listen",
			Net:    p.n.m.listener.Addr().Network(),
			Source: nil,
			Addr:   p.n.m.listener.Addr(),
			Err:    fmt.Errorf(useOfClosedErrString),
		}
	}
}

func (p *packetConn) WriteTo(buf []byte, addr net.Addr) (n int, err error) {
	framedPayload := make([]byte, tagLengthFieldSize, tagLengthFieldSize+len(p.n.tag)+len(buf))
	encoding.PutUint16(framedPayload[:tagLengthFieldSize], uint16(len(p.n.tag)))
	framedPayload = append(framedPayload, []byte(p.n.tag)...)
	framedPayload = append(framedPayload, buf...)
	session, err := p.n.m.getOrDialSession(addr.String())
	if err != nil {
		return 0, err
	}
	err = session.SendMessage(framedPayload)
	if err != nil {
		return
	}
	return len(buf), nil
}

func (p *packetConn) SetDeadline(t time.Time) error      { panic("unimplemented") }
func (p *packetConn) SetReadDeadline(t time.Time) error  { panic("unimplemented") }
func (p *packetConn) SetWriteDeadline(t time.Time) error { panic("unimplemented") }

func (n *Network) ListenPacket() (net.PacketConn, error) {
	// Add as a packet listener
	n.m.mu.Lock()
	if _, ok := n.m.packetEntries[n.tag]; !ok {
		n.m.packetEntries[n.tag] = &packetEntry{
			ch: make(chan packet, packetConnBufferSize),
		}
	}
	n.m.packetEntries[n.tag].n++
	ch := n.m.packetEntries[n.tag].ch
	n.m.mu.Unlock()

	p := &packetConn{
		n:      n,
		ch:     ch,
		closed: make(chan struct{}),
	}
	return p, nil
}
