package syncbuf

import (
	"bytes"
	"io"
	"sync"
)

type Syncbuf struct {
	mu      sync.Mutex
	stopped bool
	buf     bytes.Buffer
}

func (s *Syncbuf) Read(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return 0, io.EOF
	}
	return s.buf.Read(b)
}

func (s *Syncbuf) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return 0, io.ErrUnexpectedEOF
	}
	return s.buf.Write(b)
}

func (s *Syncbuf) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
}

func (s *Syncbuf) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = false
}

func (s *Syncbuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}
