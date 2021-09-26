package testlog

import (
	"log"
	"strings"
	"testing"

	"github.com/vsekhar/peers/internal/syncbuf"
)

type Logger struct {
	*log.Logger
	buf *syncbuf.Syncbuf
}

func New() *Logger {
	buf := &syncbuf.Syncbuf{}
	l := log.New(buf, "", log.LstdFlags|log.Lshortfile)
	return &Logger{
		Logger: l,
		buf:    buf,
	}
}

func (l *Logger) Std() *log.Logger {
	return l.Logger
}

func (l *Logger) ErrorIfEmpty(t *testing.T) {
	l.buf.Close()
	logs := l.buf.String()
	if len(logs) == 0 {
		t.Error("no logs")
	}
}

func (l *Logger) ErrorIfNotEmpty(t *testing.T) {
	l.buf.Close()
	logs := l.buf.String()
	if len(logs) > 0 {
		t.Error(logs)
	}
}

func (l *Logger) ErrorIfContains(t *testing.T, substr ...string) {
	l.buf.Close()
	logs := l.buf.String()
	for _, s := range substr {
		if strings.Contains(logs, s) {
			t.Error(logs)
			return
		}
	}
}
