package transport

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"golang.org/x/sync/singleflight"
)

const (
	// Underlying socket transport tags
	userTag = "skt"
	keyTag  = "tkt"
	keyAck  = "kak"

	// Encryption
	idLength     = 32
	keyLength    = 32   // AES-256
	keyCacheSize = 1024 // 512 keys, 2 entries per key (by id and by host)
	nonceLength  = 12
	bufferLength = 1024
)

type key [keyLength]byte
type keyID [idLength]byte

type keyEntry struct {
	key key
	id  keyID
}

type tlsWithInsecureUDPTransport struct {
	Interface
	config *tls.Config
}

func (t *tlsWithInsecureUDPTransport) Dial(network, address string) (net.Conn, error) {
	return t.DialContext(context.Background(), network, address)
}

func (t *tlsWithInsecureUDPTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := t.Interface.DialContext(ctx, network, address)
	if err != nil {
		return c, err
	}
	tc := tls.Client(c, t.config)
	return tc, nil
}

func (t *tlsWithInsecureUDPTransport) Accept() (net.Conn, error) {
	c, err := t.Interface.Accept()
	if err != nil {
		return c, err
	}
	tc := tls.Server(c, t.config)
	return tc, nil
}

func TLSWithInsecureUDP(transport Interface, config *tls.Config) Interface {
	return &tlsWithInsecureUDPTransport{
		Interface: transport,
		config:    config,
	}
}

type tlsTransport struct {
	context          context.Context
	cancel           func()
	userTransport    Interface
	keyTransport     Interface
	keys             *lru.ARCCache
	keysSingleFlight *singleflight.Group
	logger           *log.Logger
}

func (t *tlsTransport) loadKey(hostOrID string) (keyEntry, bool) {
	if v, ok := t.keys.Get(hostOrID); ok {
		return v.(keyEntry), true
	}
	return keyEntry{}, false
}

func (t *tlsTransport) storeKey(address string, id keyID, k []byte) {
	e := keyEntry{}
	copy(e.key[:], k)
	copy(e.id[:], id[:])
	t.keys.Add(address, e)
	t.keys.Add(string(id[:]), e)
}

func (t *tlsTransport) sendKey(ctx context.Context, address string) (err error) {
	c, err := t.keyTransport.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer func() {
		e := c.Close()
		if e != nil && err == nil {
			err = e
		}
	}()

	// Send key ID, which will identify the key to use for subsequent packets
	// sent to this remote host.
	var id keyID
	rand.Read(id[:])
	n, err := c.Write(id[:])
	if err != nil {
		return err
	}
	if n != idLength {
		return io.ErrShortWrite
	}

	// Send key
	tc := c.(*tls.Conn) // Guaranteed by constructor using TLSInsecureUDP as base
	cs := tc.ConnectionState()
	k, err := cs.ExportKeyingMaterial("tlstransportUDPkey", nil, keyLength)
	if err != nil {
		return err
	}
	if len(k) != keyLength {
		return err
	}
	n, err = c.Write(k)
	if err != nil {
		return err
	}
	if n != keyLength {
		return io.ErrShortWrite
	}

	// Wait for ACK
	var ackBuf [len(keyAck)]byte
	n, err = c.Read(ackBuf[:])
	if err != nil {
		return err
	}
	if n != len(keyAck) {
		return io.ErrShortWrite
	}
	if !bytes.Equal([]byte(keyAck), ackBuf[:]) {
		return fmt.Errorf("bad ack")
	}

	// Store key by address for when we're sending packets to this remote host,
	// and store key by ID for when we're we're receiving packets from this
	// remote host.
	t.storeKey(address, id, k)

	return nil
}

func (t *tlsTransport) receiveKey(c net.Conn) (err error) {
	defer func() {
		e := c.Close()
		if e != nil && err == nil {
			err = e
		}
	}()

	// Receive key ID
	var id keyID
	n, err := c.Read(id[:])
	if err != nil {
		return err
	}
	if n != idLength {
		return fmt.Errorf("expected key id %d bytes, got %d bytes", idLength, n)
	}

	// Receive key
	var k key
	n, err = c.Read(k[:])
	if err != nil {
		return err
	}
	if n != keyLength {
		return fmt.Errorf("expected key %d bytes, got %d bytes", keyLength, n)
	}

	// Send ACK
	n, err = c.Write([]byte(keyAck))
	if err != nil {
		return err
	}
	if n != len(keyAck) {
		return io.ErrShortWrite
	}
	t.storeKey(c.RemoteAddr().String(), id, k[:])
	return nil
}

func (t *tlsTransport) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	var ar [bufferLength]byte
	for {
		n, addr, err = t.userTransport.ReadFrom(ar[:])
		if err != nil {
			return
		}
		if n < idLength+nonceLength {
			continue // short read
		}
		id, nonce, cipherText :=
			ar[:idLength],
			ar[idLength:idLength+nonceLength],
			ar[idLength+nonceLength:n]
		if k, ok := t.loadKey(string(id)); ok {
			block, err := aes.NewCipher(k.key[:])
			if err != nil {
				t.logger.Printf("tlstransport: bad key")
			}
			gcm, err := cipher.NewGCM(block)
			if err != nil {
				t.logger.Printf("tlstransport: failed to create GCM")
			}
			out, err := gcm.Open(nil, nonce, cipherText, nil)
			if err != nil {
				// TODO: provoke new key exchange?
				// TODO: signal the other side?
				continue // drop packet, bad crypto
			}
			n = copy(p, out)
			return n, addr, err
		}
		// Drop packet
		// TODO: initiate key push somehow from the receiver? Does this get
		// racy with pushes initiated from the sender?
	}
}

func (t *tlsTransport) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	k, ok := t.loadKey(addr.String())
	if !ok {
		_, err, _ = t.keysSingleFlight.Do(addr.String(), func() (interface{}, error) {
			return nil, t.sendKey(context.Background(), addr.String())
		})
		if err != nil {
			return 0, err
		}
		k, ok = t.loadKey(addr.String())
		if !ok {
			// Should never happen but check anyway so we don't accidentally
			// use a zero key
			return 0, fmt.Errorf("tlstransport: expected key not found")
		}
	}

	block, err := aes.NewCipher(k.key[:])
	if err != nil {
		return 0, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return 0, err
	}
	payloadLength := idLength + nonceLength + len(p) + gcm.Overhead()
	payload := make([]byte, payloadLength)

	id, nonce, cipherText :=
		payload[:idLength],
		payload[idLength:idLength+nonceLength],
		payload[idLength+nonceLength:]
	copy(id, k.id[:])
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return 0, err
	}
	cipherText = gcm.Seal(cipherText[:0], nonce, p, nil)
	wrote, err := t.userTransport.WriteTo(payload[:idLength+nonceLength+len(cipherText)], addr)
	if err != nil {
		return 0, err
	}
	if wrote == idLength+nonceLength+len(cipherText) {
		return len(p), nil
	}
	return 0, io.ErrShortWrite
}

func (t *tlsTransport) SetDeadline(d time.Time) error { return t.userTransport.SetDeadline(d) }
func (t *tlsTransport) SetReadDeadline(d time.Time) error {
	return t.userTransport.SetReadDeadline(d)
}
func (t *tlsTransport) SetWriteDeadline(d time.Time) error {
	return t.userTransport.SetWriteDeadline(d)
}
func (t *tlsTransport) Addr() net.Addr            { return t.userTransport.Addr() }
func (t *tlsTransport) LocalAddr() net.Addr       { return t.userTransport.LocalAddr() }
func (t *tlsTransport) Accept() (net.Conn, error) { return t.userTransport.Accept() }
func (t *tlsTransport) Dial(network, address string) (net.Conn, error) {
	return t.userTransport.Dial(network, address)
}
func (t *tlsTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return t.userTransport.DialContext(ctx, network, address)
}
func (t *tlsTransport) Close() error {
	t.cancel()
	return nil
}

// TODO: UDP is _sent_ from ephemeral ports, so receiving end cannot look up key
// by remote address:port. Need to send some keyID with each message.

func TLS(ctx context.Context, transport Interface, config *tls.Config, logger *log.Logger) Interface {
	cctx, cancel := context.WithCancel(ctx)
	unsecureUDP := TLSWithInsecureUDP(transport, config)
	tagged := Tagged(unsecureUDP, logger, userTag, keyTag)
	keyCache, err := lru.NewARC(keyCacheSize)
	if err != nil {
		panic(err)
	}
	r := &tlsTransport{
		context:          cctx,
		cancel:           cancel,
		userTransport:    tagged[userTag],
		keyTransport:     tagged[keyTag],
		keys:             keyCache,
		keysSingleFlight: new(singleflight.Group),
		logger:           logger,
	}

	// Read key pushes
	go func() {
		for {
			c, err := r.keyTransport.Accept()
			if err != nil {
				logger.Printf("tlstransport: accept error: %v", err)
				continue
			}

			go func(c net.Conn) {
				if err := r.receiveKey(c); err != nil {
					logger.Printf("tlstransport: error reading key: %v", err)
				}
			}(c)

			select {
			case <-cctx.Done():
				r.keyTransport.Close()
				return
			default:
			}
		}
	}()

	return r
}
