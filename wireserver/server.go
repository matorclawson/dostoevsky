// Package wireserver serves a Dostoevsky node over the native wire protocol, version 1, as specified in
// docs/wire-v1.md. The token grants access to the whole store.
package wireserver

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/wire"
)

// maxFrame is the largest frame, header included.
const maxFrame = wire.MaxBody + wire.HeaderSize

// ErrServerClosed is what Serve returns after Close.
var ErrServerClosed = errors.New("wireserver: closed")

// Store is what the server needs from a node. *dostoevsky.Node implements it.
type Store interface {
	d.Store
	PublishBatch(context.Context, []d.Publication) ([]d.Record, error)
	Await(context.Context, d.Partition, int64) error
}

// Config sets the token and the limits. A zero limit means its default.
type Config struct {
	Token string
	// TLS, when set, is required of every connection. Without it Serve accepts only loopback and unix listeners.
	TLS *tls.Config

	MaxConnections     int // 1024
	MaxUnauthenticated int // 64
	Window             int // requests a connection may have unanswered, 256
	WindowBytes        int // request bytes a connection may have unanswered, 4 MiB
	RequestPool        int // request bytes held by the whole server, 16 MiB
	ReplyPool          int // reply bytes held by the whole server, 16 MiB
	ConnectionReplies  int // reply pool bytes one connection may hold, 2 maximum frames
	MaxWaits           int // reads waiting for a record, whole server, 4096

	AuthTimeout  time.Duration // accept to WELCOME written, 5 s
	FrameTimeout time.Duration // first header byte to last body byte, 40 s
	ReplyTimeout time.Duration // reply ready to last byte written, 45 s
	IdleTimeout  time.Duration // nothing unanswered to the next frame, 60 s
}

func fill[T int | time.Duration](v *T, def T) {
	if *v == 0 {
		*v = def
	}
}

func (c *Config) complete() error {
	fill(&c.MaxConnections, 1024)
	fill(&c.MaxUnauthenticated, 64)
	fill(&c.Window, 256)
	fill(&c.WindowBytes, 4<<20)
	fill(&c.RequestPool, 16<<20)
	fill(&c.ReplyPool, 16<<20)
	fill(&c.ConnectionReplies, 2*maxFrame)
	fill(&c.MaxWaits, 4096)
	fill(&c.AuthTimeout, 5*time.Second)
	fill(&c.FrameTimeout, 40*time.Second)
	fill(&c.ReplyTimeout, 45*time.Second)
	fill(&c.IdleTimeout, 60*time.Second)
	switch {
	case len(c.Token) < wire.MinToken || len(c.Token) > wire.MaxToken:
		return errors.New("wireserver: the token must be 16 to 4096 bytes")
	case c.MaxConnections < 1 || c.MaxUnauthenticated < 1 || c.MaxWaits < 1:
		return errors.New("wireserver: connection and wait limits must be positive")
	case c.Window < 1 || c.Window > 1<<16-1:
		return errors.New("wireserver: window must be 1 to 65535")
	case c.WindowBytes < maxFrame || uint64(c.WindowBytes) > 1<<32-1:
		return errors.New("wireserver: window bytes must hold a maximum frame and fit 32 bits")
	case c.RequestPool < maxFrame || c.ReplyPool < maxFrame || c.ConnectionReplies < maxFrame:
		return errors.New("wireserver: each pool must hold a maximum frame")
	case c.AuthTimeout < 0 || c.FrameTimeout < 0 || c.ReplyTimeout < 0 || c.IdleTimeout < 0:
		return errors.New("wireserver: timeouts must be positive")
	}
	if c.TLS != nil {
		c.TLS = c.TLS.Clone()
		if c.TLS.MinVersion < tls.VersionTLS12 {
			c.TLS.MinVersion = tls.VersionTLS12
		}
	}
	return nil
}

// Server serves one Store.
type Server struct {
	store    Store
	cfg      Config
	token    []byte
	requests pool
	replies  pool
	writes   sequencer
	waits    chan struct{} // one slot for each waiting read

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // connections and their requests

	mu        sync.Mutex
	closed    bool
	listeners map[net.Listener]struct{}
	conns     map[net.Conn]struct{}
	unauth    int
}

// New checks the configuration and starts the writer. Call Close before closing the store.
func New(store Store, c Config) (*Server, error) {
	if store == nil {
		return nil, d.ErrInvalid
	}
	if err := c.complete(); err != nil {
		return nil, err
	}
	s := &Server{store: store, cfg: c, token: []byte(c.Token),
		listeners: make(map[net.Listener]struct{}), conns: make(map[net.Conn]struct{})}
	s.waits = make(chan struct{}, c.MaxWaits)
	s.requests.free = c.RequestPool
	s.replies.free = c.ReplyPool
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.writes.start(store)
	return s, nil
}

// plaintextAllowed reports whether a listener's bound address stays on this machine.
func plaintextAllowed(a net.Addr) bool {
	switch v := a.(type) {
	case *net.UnixAddr:
		return v.Net == "unix"
	case *net.TCPAddr:
		return v.IP.IsLoopback()
	}
	return false
}

// Serve accepts connections until Close. Pass a plain listener: TLS is added from the Config.
func (s *Server) Serve(ln net.Listener) error {
	if s.cfg.TLS == nil && !plaintextAllowed(ln.Addr()) {
		ln.Close()
		return errors.New("wireserver: refusing to serve the whole-store token in plaintext off this machine")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return ErrServerClosed
	}
	s.listeners[ln] = struct{}{}
	s.mu.Unlock()
	for {
		raw, err := ln.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return ErrServerClosed
			}
			if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) || errors.Is(err, syscall.ECONNABORTED) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		s.accept(raw)
	}
}

// accept takes a slot for a new connection, or closes it at once.
func (s *Server) accept(raw net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.conns) >= s.cfg.MaxConnections || s.unauth >= s.cfg.MaxUnauthenticated {
		raw.Close()
		return
	}
	s.conns[raw] = struct{}{}
	s.unauth++
	s.wg.Add(1)
	go s.serve(raw, time.Now())
}

// serve authenticates one connection and then runs it.
func (s *Server) serve(raw net.Conn, accepted time.Time) {
	defer s.wg.Done()
	nc := raw
	if s.cfg.TLS != nil {
		nc = tls.Server(raw, s.cfg.TLS)
	}
	ok := s.handshake(nc, accepted.Add(s.cfg.AuthTimeout))
	s.mu.Lock()
	s.unauth--
	s.mu.Unlock()
	if ok {
		newConn(s, raw, nc).run()
	} else {
		nc.Close()
	}
	s.mu.Lock()
	delete(s.conns, raw)
	s.mu.Unlock()
}

func (s *Server) handshake(nc net.Conn, deadline time.Time) bool {
	if nc.SetDeadline(deadline) != nil {
		return false
	}
	h, err := wire.ReadHello(nc)
	if err != nil {
		return false
	}
	w := wire.Welcome{Status: wire.Authenticate(h, s.token)}
	if w.Status == wire.OK {
		w.Version = wire.Version
		w.MaxBody = wire.MaxBody
		w.Window = uint16(s.cfg.Window)
		w.WindowBytes = uint32(s.cfg.WindowBytes)
	}
	if _, err := nc.Write(wire.AppendWelcome(nil, w)); err != nil || w.Status != wire.OK {
		return false
	}
	return nc.SetDeadline(time.Time{}) == nil
}

// Close stops accepting, closes every connection and waits. Writes already admitted run to completion.
func (s *Server) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.cancel()
		for ln := range s.listeners {
			ln.Close()
		}
		for raw := range s.conns {
			raw.Close()
		}
	}
	s.mu.Unlock()
	s.wg.Wait()
	s.writes.close()
	return nil
}
