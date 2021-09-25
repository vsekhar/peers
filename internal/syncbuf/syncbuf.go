package syncbuf

import (
	"bytes"
	"io"
	"sync"
)

type Syncbuf struct {
	mu     sync.Mutex
	closed bool
	buf    bytes.Buffer
}

func (s *Syncbuf) Read(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, io.EOF
	}
	return s.buf.Read(b)
}

func (s *Syncbuf) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, io.ErrUnexpectedEOF
	}
	return s.buf.Write(b)
}

func (s *Syncbuf) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *Syncbuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}
