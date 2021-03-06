package testlog

import (
	"log"
	"strings"
	"testing"

	"github.com/vsekhar/peers/internal/syncbuf"
)

type Logger struct {
	l   *log.Logger
	buf *syncbuf.Syncbuf
}

func New() *Logger {
	buf := &syncbuf.Syncbuf{}
	l := log.New(buf, "", log.LstdFlags|log.Lshortfile)
	return &Logger{
		l:   l,
		buf: buf,
	}
}

func (l *Logger) Std() *log.Logger {
	return l.l
}

func (l *Logger) ErrorIfEmpty(t *testing.T) {
	t.Helper()
	l.buf.Stop()
	defer l.buf.Start()
	logs := l.buf.String()
	if len(logs) == 0 {
		t.Error("no logs")
	}
}

func (l *Logger) ErrorIfNotEmpty(t *testing.T) {
	t.Helper()
	l.buf.Stop()
	defer l.buf.Start()
	logs := l.buf.String()
	if len(logs) > 0 {
		t.Error(logs)
	}
}

func (l *Logger) ErrorIfContains(t *testing.T, substr ...string) {
	t.Helper()
	l.buf.Stop()
	defer l.buf.Start()
	logs := l.buf.String()
	failed := false
	for _, s := range substr {
		if strings.Contains(logs, s) {
			failed = true
			t.Errorf("Logs contain '%s'", s)
		}
	}
	if failed {
		t.Error(logs)
	}
}

func (l *Logger) ErrorIfContainsMoreThan(t *testing.T, substr string, n int) {
	t.Helper()
	l.buf.Stop()
	defer l.buf.Start()
	logs := l.buf.String()
	if strings.Count(logs, substr) > n {
		t.Error(logs)
	}
}

func (l *Logger) ErrorIfNotContainsAnyOf(t *testing.T, substr ...string) {
	t.Helper()
	l.buf.Stop()
	defer l.buf.Start()
	logs := l.buf.String()
	failed := false
	for _, s := range substr {
		if !strings.Contains(logs, s) {
			failed = true
			t.Errorf("Logs do not contain '%s'", s)
		}
	}
	if failed {
		t.Error(logs)
	}
}

func (l *Logger) Flush() string {
	l.buf.Stop()
	defer l.buf.Start()
	return l.buf.String()
}

func (l *Logger) Clear() {
	l.buf.Stop()
	defer l.buf.Start()
	l.buf = &syncbuf.Syncbuf{}
}

type Printfer interface {
	Print(...interface{})
	Printf(string, ...interface{})
	Println(string, ...interface{})
}

type tWrapper struct{ *testing.T }

func (t *tWrapper) Print(x ...interface{})             { t.Log(x...) }
func (t *tWrapper) Printf(s string, x ...interface{})  { t.Logf(s, x...) }
func (t *tWrapper) Println(s string, x ...interface{}) { t.Logf(s, x...) }

// Wrap wraps a *testing.T so that it has Print{,f,ln} methods, behaving more
// like a *log.Logger.
func Wrap(t *testing.T) Printfer { return &tWrapper{t} }
