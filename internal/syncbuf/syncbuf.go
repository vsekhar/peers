package syncbuf

import (
	"bytes"
	"sync"
)

type Syncbuf struct {
	mu      sync.Mutex
	cond    sync.Cond // lazily initialized
	stopped bool
	buf     bytes.Buffer
}

func (s *Syncbuf) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.stopped {
		if s.cond.L == nil {
			s.cond.L = &s.mu
		}
		s.cond.Wait()
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
	if s.cond.L != nil {
		s.cond.Broadcast()
	}
}

func (s *Syncbuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}
