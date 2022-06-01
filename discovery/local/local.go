package local

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/vsekhar/peers/discovery"
	"github.com/vsekhar/peers/internal/atomic"
)

const maxEntryAge = 30 * time.Second
const maxFileAge = 60 * time.Second

func GetBinarySpecificPath() string {
	// For testing
	execPath, err := os.Executable()
	if err != nil {
		panic(err)
	}
	execPath = filepath.Base(execPath)
	path := filepath.Join(os.TempDir(), execPath)
	s, err := os.Stat(path)
	if err != nil {
		return path
	}
	if time.Since(s.ModTime()) > maxFileAge {
		os.RemoveAll(path)
	}
	return path
}

type localUnixDomainSocketDiscoverer struct {
	path   string
	me     string
	logger *log.Logger
}

func New(ctx context.Context, path, me string, logger *log.Logger) (discovery.Interface, error) {
	return &localUnixDomainSocketDiscoverer{
		path:   path,
		me:     me,
		logger: logger,
	}, nil
}

type entry struct {
	addrPort string
	seconds  int64
}

func (l *localUnixDomainSocketDiscoverer) Discover(ctx context.Context) string {
	f, err := os.Open(l.path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			l.logger.Printf("peers-discover-local opening %s: %v", l.path, err)
			return ""
		}
		f, err = os.Create(l.path)
		if err != nil {
			l.logger.Printf("peers-discover-local creating %s: %v", l.path, err)
			return ""
		}
	}

	// read existing, pick random row
	scanner := bufio.NewScanner(f)
	entries := make([]entry, 0)
	for scanner.Scan() {
		t := scanner.Text()

		// entries are <address:port> <timestamp>
		// e.g. 127.0.0.1:5493 428792859837
		parts := strings.Split(t, " ")
		if len(parts) != 2 {
			l.logger.Printf("peers-discover-local bad entry '%s' in %s: %v", t, l.path, err)
			continue
		}
		addrPort, tStr := parts[0], parts[1]
		if addrPort == l.me {
			continue
		}
		seconds, err := strconv.ParseInt(tStr, 10, 64)
		if err != nil {
			l.logger.Printf("peers-discover-local bad entry '%s' in %s: %v", t, l.path, err)
			continue
		}
		if time.Since(time.Unix(seconds, 0)) > maxEntryAge {
			continue
		}
		entries = append(entries, entry{addrPort, seconds})
	}
	if err := scanner.Err(); err != nil {
		l.logger.Printf("peers-discover-local scanning %s: %v", l.path, err)
		return ""
	}

	var e entry
	if len(entries) > 0 {
		e = entries[rand.Intn(len(entries))]
	}
	entries = append(entries, entry{l.me, time.Now().Unix()})
	af, err := atomic.Open(l.path)
	if err != nil {
		l.logger.Printf("peers-discover-local opening updated file %s: %v", l.path, err)
		return ""
	}
	for _, e := range entries {
		fmt.Fprintf(af, "%s %d\n", e.addrPort, e.seconds)
	}
	if err := af.Close(); err != nil {
		l.logger.Printf("peers-discover-local committing updated file %s: %v", l.path, err)
		return ""
	}
	return e.addrPort
}
