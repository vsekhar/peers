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

const quicTimeout = 30 * time.Second

var defaultQUICConfig = &quic.Config{
	EnableDatagrams: true,
}

type quicMessage struct {
	d    []byte
	addr net.Addr
}

type quicConn struct {
	quic.Stream
	session quic.Session
}

var _ net.Conn = (*quicConn)(nil)

func (q *quicConn) Close() error         { return q.Stream.Close() }
func (q *quicConn) LocalAddr() net.Addr  { return q.session.LocalAddr() }
func (q *quicConn) RemoteAddr() net.Addr { return q.session.RemoteAddr() }

type quicSession2 struct {
	ctx    context.Context
	cancel func()
	qSess  quic.Session
}

func newQUICSession2(ctx context.Context, session quic.Session, streams chan<- *quicConn, messages chan<- *quicMessage) *quicSession2 {
	sessCtx, cancel := context.WithCancel(ctx)

	// Stream pump
	go func() {
		for {
			stream, err := session.AcceptStream(sessCtx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				session.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), err.Error())
				return
			}
			go func(stream quic.Stream) {
				select {
				case streams <- &quicConn{stream, session}:
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
			msg, err := session.ReceiveMessage()
			if errors.Is(err, context.Canceled) {
				break
			}
			if err != nil {
				session.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), err.Error())
				return
			}
			go func(qm *quicMessage) {
				select {
				case messages <- qm:
				case <-sessCtx.Done():
				}
			}(&quicMessage{msg, session.RemoteAddr()})
		}
	}()

	return &quicSession2{sessCtx, cancel, session}
}

func (qs *quicSession2) Close() error {
	qs.cancel()
	return nil
}

// quicSessionManager manages underlying QUIC sessions (connections) between hosts.
//
// Sessions may be created in the outbound or inbound direction and are matched
// using a GUID generated for each host.
//
// When an outbound session to a host is requested, the GUID cache is checked
// to see if there is a known GUID for that host. If found, the GUID is used to
// check if there is an active inbound or outbound session for that host. If
// found, that active session is used. If the GUID is not found, the host is
// dialed, its GUID obtained and cached.
//
// quicSessionManager listens for inbound sessions via the provided transport. When
// a new session arrives, the GUID of the originating host is read and checked
//
// quicSessionManager does not itself implement Interface, but is used by
// quicTransport to implement Interface.
type quicSessionManager struct {
	hostID    uuid.UUID
	ctx       context.Context
	cancel    func()
	tlsConfig *tls.Config
	transport Interface

	cmu      *sync.Mutex
	hostIDs  map[string][16]byte        // network:address --> peer hostID
	sessions map[[16]byte]*quicSession2 // peer hostID --> quicSession
	streams  chan *quicConn
	messages chan *quicMessage
}

func newQUICSessionManager(ctx context.Context, transport Interface, tlsConfig *tls.Config) *quicSessionManager {
	ctx, cancel := context.WithCancel(ctx)
	r := &quicSessionManager{
		hostID:    uuid.New(),
		ctx:       ctx,
		cancel:    cancel,
		tlsConfig: tlsConfig,
		transport: transport,
		cmu:       new(sync.Mutex),
		hostIDs:   make(map[string][16]byte),
		sessions:  make(map[[16]byte]*quicSession2),
		streams:   make(chan *quicConn),
		messages:  make(chan *quicMessage),
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
			go func(s quic.Session) {
				// Receive then send IDs (opposite order than in getSession)
				peerHostID, err := recvID(ctx, s)
				if err != nil {
					s.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), err.Error())
					return
				}
				if err := sendID(ctx, s, r.hostID); err != nil {
					s.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), err.Error())
					return
				}

				session := newQUICSession2(ctx, s, r.streams, r.messages) // start after sendID & recvID
				r.cmu.Lock()
				if oldSessionHostID, ok := r.hostIDs[s.RemoteAddr().String()]; ok {
					if bytes.Equal(oldSessionHostID[:], peerHostID[:]) {
						if oldSession, ok := r.sessions[oldSessionHostID]; ok {
							if err := oldSession.Close(); err != nil {
								log.Printf("closing old session: %s", err)
							}
						} else {
							panic("transport/quic: hostIDs and sessions out of sync")
						}
					}
				}
				r.hostIDs[s.RemoteAddr().String()] = peerHostID
				r.sessions[peerHostID] = session
				r.cmu.Unlock()
			}(s)

		}
	}()

	return r
}

func (q *quicSessionManager) getSession(ctx context.Context, _, address string) (*quicSession2, error) {
	// If we've dialed this host before, reuse the session
	var sess *quicSession2
	ok := false
	q.cmu.Lock()
	hostID, ok := q.hostIDs[address]
	if ok {
		sess, ok = q.sessions[hostID]
		if !ok {
			panic("transport/quic: hostIDs and sessions out of sync")
		}
		q.cmu.Unlock()
		return sess, nil
	}
	q.cmu.Unlock()

	// Create a new session
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	session, err := quic.DialContext(ctx, q.transport, addr, "", q.tlsConfig, defaultQUICConfig)
	if err != nil {
		return nil, err
	}
	// Send then receive IDs (opposite order from newQUICSessionManager)
	if err := sendID(ctx, session, q.hostID); err != nil {
		session.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), "")
		return nil, err
	}
	peerHostID, err := recvID(ctx, session)
	if err != nil {
		session.CloseWithError(quic.ApplicationErrorCode(quic.InternalError), "")
		return nil, err
	}
	q.cmu.Lock()
	sess = newQUICSession2(q.ctx, session, q.streams, q.messages)
	q.hostIDs[address] = peerHostID
	q.sessions[peerHostID] = sess
	q.cmu.Unlock()
	return sess, nil
}

func (q *quicSessionManager) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	s, err := q.getSession(ctx, network, address)
	if err != nil {
		return nil, err
	}
	qc, err := s.qSess.OpenStreamSync(ctx)
	if err != nil {
		qc.Close()
		s.Close()
		return nil, err
	}
	return &quicConn{
		Stream:  qc,
		session: s.qSess,
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
	err = s.qSess.SendMessage(p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// TODO: use singleflight to create sessions to a host once

func sendID(ctx context.Context, s quic.Session, hostID uuid.UUID) error {
	stream, err := s.OpenUniStream()
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

func recvID(ctx context.Context, s quic.Session) (peerID uuid.UUID, err error) {
	acceptCtx, cancel := context.WithTimeout(ctx, quicTimeout)
	defer cancel()
	stream, err := s.AcceptUniStream(acceptCtx)
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