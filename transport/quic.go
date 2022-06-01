package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lucas-clemente/quic-go"
)

// This file contains a transport implementation that uses QUIC to secure both
// TCP and UDP traffic.

const quicTimeout = 30 * time.Second // TODO: do we need this?

var defaultQUICConfig = &quic.Config{
	EnableDatagrams: true,
}

type quicMessage struct {
	d    []byte
	addr net.Addr
}

// quicConn emulates a net.Conn using a quic.Stream.
type quicConn struct {
	quic.Stream
	conn quic.Connection
}

var _ net.Conn = (*quicConn)(nil)

func (q *quicConn) Close() error         { return q.Stream.Close() }
func (q *quicConn) LocalAddr() net.Addr  { return q.conn.LocalAddr() }
func (q *quicConn) RemoteAddr() net.Addr { return q.conn.RemoteAddr() }

type quicConnection struct {
	ctx    context.Context
	cancel func()
	qConn  quic.Connection
}

func newQUICConnection(ctx context.Context, conn quic.Connection, streams chan<- *quicConn, messages chan<- *quicMessage) *quicConnection {
	sessCtx, cancel := context.WithCancel(ctx)

	// Stream pump
	go func() {
		for {
			stream, err := conn.AcceptStream(sessCtx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				conn.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), err.Error())
				return
			}
			go func(stream quic.Stream) {
				select {
				case streams <- &quicConn{stream, conn}:
				case <-time.After(quicTimeout):
					stream.Close()
				case <-sessCtx.Done():
					stream.Close()
				}
			}(stream)
		}
	}()

	// Message pump
	go func() {
		for {
			msg, err := conn.ReceiveMessage()
			if errors.Is(err, context.Canceled) {
				break
			}
			if err != nil {
				conn.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), err.Error())
				return
			}
			go func(qm *quicMessage) {
				select {
				case messages <- qm:
				case <-sessCtx.Done():
				}
			}(&quicMessage{msg, conn.RemoteAddr()})
		}
	}()

	return &quicConnection{sessCtx, cancel, conn}
}

func (qs *quicConnection) Close() error {
	qs.cancel()
	return nil
}

// QUIC contains two layers of abstraction (connection and stream) whereas other
// network protocols contain only one (socket). To handle this, we cache
// QUIC connections and re-use them when dialing or listening for new streams, and
// we pass streams on to the application to be used as traditional sockets.
// Since we cannot trust network addresses due to NAT, each host creates a GUID
// and sends it to the other on the first stream when creating a connection. If
// a new incoming connection arrives for a given peer GUID, hosts must terminate
// the old connection.

// quicConnectionManager manages underlying QUIC connections between hosts.
//
// Connections may be created in the outbound or inbound direction and are matched
// using a GUID generated for each host.
//
// When an outbound connection to a host is requested, the GUID cache is checked
// to see if there is a known GUID for that host. If found, the GUID is used to
// check if there is an active inbound or outbound connection for that host. If
// found, that active connection is used. If the GUID is not found, the host is
// dialed, its GUID obtained and cached.
//
// quicSessionManager listens for inbound connections via the provided transport. When
// a new connection arrives, the GUID of the originating host is read and checked
//
// quicSessionManager does not itself implement Interface, but is used by
// quicTransport to implement Interface.
type quicSessionManager struct {
	Interface
	hostID    uuid.UUID
	ctx       context.Context
	cancel    func()
	tlsConfig *tls.Config

	cmu         *sync.Mutex
	hostIDs     map[string][16]byte          // network:address --> peer hostID
	connections map[[16]byte]*quicConnection // peer hostID --> quicConnection
	streams     chan *quicConn
	messages    chan *quicMessage
}

func QUIC(transport Interface, tlsConfig *tls.Config) Interface {
	ctx, cancel := context.WithCancel(context.Background())
	r := &quicSessionManager{
		Interface:   transport,
		hostID:      uuid.New(),
		ctx:         ctx,
		cancel:      cancel,
		tlsConfig:   tlsConfig,
		cmu:         new(sync.Mutex),
		hostIDs:     make(map[string][16]byte),
		connections: make(map[[16]byte]*quicConnection),
		streams:     make(chan *quicConn),
		messages:    make(chan *quicMessage),
	}

	// Session pump
	go func() {
		l, err := quic.Listen(transport, tlsConfig, defaultQUICConfig)
		if err != nil {
			log.Print(err)
			return
		}
		for {
			s, err := l.Accept(r.ctx)
			if err != nil {
				log.Print(err)
				continue
			}
			go func(c quic.Connection) {
				// Receive then send IDs (opposite order than in getSession)
				peerHostID, err := recvID(ctx, c)
				if err != nil {
					c.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), err.Error())
					return
				}
				if err := sendID(ctx, c, r.hostID); err != nil {
					c.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), err.Error())
					return
				}

				connection := newQUICConnection(ctx, c, r.streams, r.messages) // start after sendID & recvID
				r.cmu.Lock()
				if oldSessionHostID, ok := r.hostIDs[c.RemoteAddr().String()]; ok {
					if bytes.Equal(oldSessionHostID[:], peerHostID[:]) {
						if oldSession, ok := r.connections[oldSessionHostID]; ok {
							if err := oldSession.Close(); err != nil {
								log.Printf("closing old connection: %s", err)
							}
						} else {
							panic("transport/quic: hostIDs and connections out of sync")
						}
					}
				}
				r.hostIDs[c.RemoteAddr().String()] = peerHostID
				r.connections[peerHostID] = connection
				r.cmu.Unlock()
			}(s)

		}
	}()

	return r
}

func (q *quicSessionManager) getSession(ctx context.Context, _, address string) (*quicConnection, error) {
	// If we've dialed this host before, reuse the connection
	var sess *quicConnection
	ok := false
	q.cmu.Lock()
	hostID, ok := q.hostIDs[address]
	if ok {
		sess, ok = q.connections[hostID]
		if !ok {
			panic("transport/quic: hostIDs and connections out of sync")
		}
		q.cmu.Unlock()
		return sess, nil
	}
	q.cmu.Unlock()

	// Create a new connection
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	connection, err := quic.DialContext(ctx, q.Interface, addr, "", q.tlsConfig, defaultQUICConfig)
	if err != nil {
		return nil, err
	}
	// Send then receive IDs (opposite order from newQUICSessionManager)
	if err := sendID(ctx, connection, q.hostID); err != nil {
		connection.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), "")
		return nil, err
	}
	peerHostID, err := recvID(ctx, connection)
	if err != nil {
		connection.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), "")
		return nil, err
	}
	q.cmu.Lock()
	sess = newQUICConnection(q.ctx, connection, q.streams, q.messages)
	q.hostIDs[address] = peerHostID
	q.connections[peerHostID] = sess
	q.cmu.Unlock()
	return sess, nil
}

func (q *quicSessionManager) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	s, err := q.getSession(ctx, network, address)
	if err != nil {
		return nil, err
	}
	qc, err := s.qConn.OpenStreamSync(ctx)
	if err != nil {
		qc.Close()
		s.Close()
		return nil, err
	}
	return &quicConn{
		Stream: qc,
		conn:   s.qConn,
	}, nil
}

func (q *quicSessionManager) Accept() (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), quicTimeout)
	defer cancel()
	var stream *quicConn
	select {
	case stream = <-q.streams:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return stream, nil
}

func (q *quicSessionManager) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	qm := <-q.messages
	n = copy(p, qm.d)
	return n, qm.addr, nil
}

func (q *quicSessionManager) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	s, err := q.getSession(q.ctx, addr.Network(), addr.String())
	if err != nil {
		return 0, err
	}
	err = s.qConn.SendMessage(p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (q *quicSessionManager) Close() error {
	q.cancel()
	return nil
}

// TODO: use singleflight to create connections to a host once

func sendID(ctx context.Context, c quic.Connection, hostID uuid.UUID) error {
	stream, err := c.OpenUniStream()
	if err != nil {
		return err
	}
	defer stream.Close()
	if err = stream.SetWriteDeadline(time.Now().Add(quicTimeout)); err != nil {
		return err
	}
	_, err = stream.Write(hostID[:])
	if err != nil {
		return err
	}
	return nil
}

func recvID(ctx context.Context, c quic.Connection) (peerID uuid.UUID, err error) {
	acceptCtx, cancel := context.WithTimeout(ctx, quicTimeout)
	defer cancel()
	stream, err := c.AcceptUniStream(acceptCtx)
	if err != nil {
		return uuid.UUID{}, err
	}
	defer stream.CancelRead(quic.StreamErrorCode(quic.NoError))
	n, err := stream.Read(peerID[:])
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if err != nil || n == 0 {
		return uuid.UUID{}, err
	}
	return
}
