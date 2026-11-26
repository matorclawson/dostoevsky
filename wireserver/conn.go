package wireserver

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/wire"
)

// reuseGrace is how long a frame that reuses the id of a reply being written waits for that write to return. A client
// holding the whole reply may reuse the id before the server's write has returned; one that cannot have it yet may not.
const reuseGrace = time.Second

// request is what a connection holds for one admitted frame until its reply is written.
type request struct {
	q    wire.Request
	size int // frame bytes, header included
}

// outgoing is a reply waiting to be written.
type outgoing struct {
	frame  []byte
	id     uint32
	size   int  // window bytes to free; 0 for a connection error, which answers no request
	budget bool // holds a maximum frame of the reply pools
	fatal  bool // close after writing it
	ready  time.Time
}

type conn struct {
	s       *Server
	raw     net.Conn // the accepted connection; nc is it, or TLS over it
	nc      net.Conn
	ctx     context.Context
	cancel  context.CancelFunc
	replies pool          // this connection's share of the reply pool
	wake    chan struct{} // a reply was queued
	freed   chan struct{} // window was given back

	mu      sync.Mutex
	closed  bool
	failed  bool // a connection error is queued; nothing may follow it
	inFrame bool // part of a frame has arrived
	count   int  // unanswered requests
	bytes   int  // their frame bytes
	ids     map[uint32]bool // unanswered ids; true while the reply is being written
	queue   []outgoing
}

func newConn(s *Server, raw, nc net.Conn) *conn {
	c := &conn{s: s, raw: raw, nc: nc, wake: make(chan struct{}, 1), freed: make(chan struct{}, 1), ids: make(map[uint32]bool)}
	c.replies.free = s.cfg.ConnectionReplies
	c.ctx, c.cancel = context.WithCancel(s.ctx)
	return c
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// run reads frames until the connection ends, and returns once its writer has stopped.
func (c *conn) run() {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.write()
	}()
	err := c.read()
	var fatal *connError
	if errors.As(err, &fatal) {
		c.fail(fatal.code)
	} else {
		c.close()
	}
	<-done
	c.close()
}

// connError ends the connection after an ERROR with request_id 0.
type connError struct{ code wire.Code }

func (e *connError) Error() string { return "wireserver: connection " + e.code.String() }

// idle arms the read deadline between frames. It is called with mu held.
func (c *conn) idle() {
	if c.inFrame || c.closed {
		return
	}
	var deadline time.Time
	if c.count == 0 {
		deadline = time.Now().Add(c.s.cfg.IdleTimeout)
	}
	_ = c.nc.SetReadDeadline(deadline)
}

func (c *conn) read() error {
	br := bufio.NewReaderSize(c.nc, 4096)
	var header [wire.HeaderSize]byte
	for {
		c.mu.Lock()
		c.inFrame = false
		c.idle()
		c.mu.Unlock()
		if _, err := br.Peek(1); err != nil {
			return err
		}
		deadline := time.Now().Add(c.s.cfg.FrameTimeout)
		c.mu.Lock()
		c.inFrame = true
		err := c.nc.SetReadDeadline(deadline)
		c.mu.Unlock()
		if err != nil {
			return err
		}
		if _, err := io.ReadFull(br, header[:]); err != nil {
			return err
		}
		if err := c.frame(br, header[:], deadline); err != nil {
			return err
		}
	}
}

// frame admits, reads and dispatches the frame whose header is given. Everything the header can show to be wrong is
// refused before anything is taken or allocated for the body.
func (c *conn) frame(br *bufio.Reader, header []byte, deadline time.Time) error {
	h, err := wire.ParseHeader(header)
	if errors.Is(err, wire.ErrTooLong) {
		return &connError{wire.Limit}
	}
	if err != nil || h.RequestID == 0 || h.Type < wire.Publish || h.Type > wire.Ping {
		return &connError{wire.Protocol}
	}
	size := int(h.Length) + wire.HeaderSize
	ctx, cancel := context.WithDeadline(c.ctx, deadline)
	defer cancel()
	if err := c.admit(ctx, h.RequestID, size); err != nil {
		return err
	}
	if !c.s.requests.take(size, ctx.Done()) {
		return ctx.Err()
	}
	body := make([]byte, h.Length)
	if _, err := io.ReadFull(br, body); err != nil {
		c.s.requests.give(size)
		return err
	}
	q, err := wire.DecodeRequest(h, body)
	var refused *wire.RequestError
	switch {
	case errors.As(err, &refused):
		c.s.requests.give(size)
		c.answer(wire.Reply{Type: wire.Error, RequestID: h.RequestID, Code: refused.Code}, size, false)
	case err != nil:
		c.s.requests.give(size)
		return &connError{wire.Protocol}
	default:
		c.dispatch(request{q, size})
	}
	return nil
}

// admit waits until the frame fits the window. An id stays taken until its reply has been written.
func (c *conn) admit(ctx context.Context, id uint32, size int) error {
	var grace <-chan time.Time
	for {
		c.mu.Lock()
		writing, used := c.ids[id]
		if used && !writing {
			c.mu.Unlock()
			return &connError{wire.Protocol}
		}
		if !used && c.count < c.s.cfg.Window && c.bytes+size <= c.s.cfg.WindowBytes {
			c.ids[id] = false
			c.count++
			c.bytes += size
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		switch {
		case !used:
			grace = nil
		case grace == nil:
			grace = time.After(reuseGrace)
		}
		select {
		case <-c.freed:
		case <-grace:
			// The write may have returned as the timer fired.
			c.mu.Lock()
			_, used = c.ids[id]
			c.mu.Unlock()
			if used {
				return &connError{wire.Protocol}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// dispatch starts the work. It is called in arrival order, which is the order writes take effect.
func (c *conn) dispatch(r request) {
	switch r.q.Type {
	case wire.Publish, wire.Commit:
		c.s.writes.add(write{c, r})
	case wire.Ping:
		c.s.requests.give(r.size)
		c.answer(wire.Reply{Type: wire.PingOK, RequestID: r.q.RequestID}, r.size, false)
	default:
		// The decoded strings are copies and these requests carry no value, so the frame is no longer held.
		c.s.requests.give(r.size)
		c.s.wg.Add(1)
		go func() {
			defer c.s.wg.Done()
			if r.q.Type == wire.Read {
				c.serveRead(r)
			} else {
				c.servePosition(r)
			}
		}()
	}
}

// reserve takes a whole maximum frame, from this connection's share and then from the server's pool.
func (c *conn) reserve() bool {
	if !c.replies.take(maxFrame, c.ctx.Done()) {
		return false
	}
	if !c.s.replies.take(maxFrame, c.ctx.Done()) {
		c.replies.give(maxFrame)
		return false
	}
	return true
}

func (c *conn) unreserve() {
	c.s.replies.give(maxFrame)
	c.replies.give(maxFrame)
}

func partition(q wire.Request) d.Partition {
	return d.Partition{Topic: q.Topic, Number: int(q.Partition)}
}

func (c *conn) refuse(r request, err error) {
	c.answer(wire.Reply{Type: wire.Error, RequestID: r.q.RequestID, Code: code(err)}, r.size, false)
}

func (c *conn) serveRead(r request) {
	p, offset := partition(r.q), int64(r.q.Offset)
	empty := wire.Reply{Type: wire.ReadOK, RequestID: r.q.RequestID}
	full := false // no slot to wait in
	if r.q.WaitMS > 0 {
		select {
		case c.s.waits <- struct{}{}:
			// Wait without holding any reply budget.
			ctx, cancel := context.WithTimeout(c.ctx, time.Duration(r.q.WaitMS)*time.Millisecond)
			err := c.s.store.Await(ctx, p, offset)
			expired := errors.Is(ctx.Err(), context.DeadlineExceeded)
			cancel()
			<-c.s.waits
			if expired && errors.Is(err, context.DeadlineExceeded) {
				c.answer(empty, r.size, false)
				return
			}
			if err != nil {
				c.refuse(r, err)
				return
			}
		default:
			full = true // a record that is already there is still served
		}
	}
	if !c.reserve() {
		return // the connection is closing
	}
	rec, err := c.s.store.Read(c.ctx, p, offset, false)
	if err != nil {
		c.unreserve()
		switch {
		case errors.Is(err, d.ErrEmpty) && full:
			c.refuse(r, d.ErrLimit)
		case errors.Is(err, d.ErrEmpty):
			c.answer(empty, r.size, false)
		default:
			c.refuse(r, err)
		}
		return
	}
	c.answer(wire.Reply{Type: wire.ReadOK, RequestID: r.q.RequestID, Found: true, Partition: uint16(rec.Number),
		Offset: uint64(rec.Offset), Time: rec.Time.UnixNano(), Key: rec.Key, MessageID: rec.ID, Value: rec.Value}, r.size, true)
}

func (c *conn) servePosition(r request) {
	if !c.reserve() {
		return
	}
	pos, err := c.s.store.Position(c.ctx, partition(r.q), r.q.Group)
	if err != nil {
		c.unreserve()
		c.refuse(r, err)
		return
	}
	c.answer(wire.Reply{Type: wire.PositionOK, RequestID: r.q.RequestID, Offset: uint64(pos.Next), Value: pos.State}, r.size, true)
}

// code maps a store error to the protocol's codes. Anything unknown is a storage failure; no message is sent.
func code(err error) wire.Code {
	for _, v := range []struct {
		err  error
		code wire.Code
	}{
		{d.ErrInvalid, wire.Invalid},
		{d.ErrOwner, wire.WrongOwner},
		{d.ErrConflict, wire.Conflict},
		{d.ErrExpired, wire.Expired},
		{d.ErrLimit, wire.Limit},
		{d.ErrClosed, wire.Closed},
		{context.DeadlineExceeded, wire.Timeout},
		{context.Canceled, wire.Canceled},
	} {
		if errors.Is(err, v.err) {
			return v.code
		}
	}
	return wire.StorageFailure
}

// answer queues the reply to an admitted request. It never blocks.
func (c *conn) answer(p wire.Reply, size int, budget bool) {
	frame, err := wire.AppendReply(nil, p)
	if err != nil {
		// A stored record the protocol cannot carry.
		frame, _ = wire.AppendReply(nil, wire.Reply{Type: wire.Error, RequestID: p.RequestID, Code: wire.StorageFailure})
	}
	c.mu.Lock()
	if c.closed || c.failed {
		c.mu.Unlock()
		if budget {
			c.unreserve()
		}
		return
	}
	c.queue = append(c.queue, outgoing{frame: frame, id: p.RequestID, size: size, budget: budget, ready: time.Now()})
	c.mu.Unlock()
	signal(c.wake)
}

// fail queues the connection error behind the replies already waiting.
func (c *conn) fail(reason wire.Code) {
	frame, _ := wire.AppendReply(nil, wire.Reply{Type: wire.Error, Code: reason})
	c.mu.Lock()
	if !c.closed {
		c.failed = true
		c.queue = append(c.queue, outgoing{frame: frame, fatal: true, ready: time.Now()})
	}
	c.mu.Unlock()
	signal(c.wake)
}

// write sends replies in the order they became ready.
func (c *conn) write() {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		if len(c.queue) == 0 {
			c.mu.Unlock()
			select {
			case <-c.wake:
			case <-c.ctx.Done():
				c.close()
			}
			continue
		}
		o := c.queue[0]
		c.queue[0] = outgoing{}
		c.queue = c.queue[1:]
		if o.size > 0 {
			c.ids[o.id] = true
		}
		c.mu.Unlock()
		err := c.nc.SetWriteDeadline(o.ready.Add(c.s.cfg.ReplyTimeout))
		if err == nil {
			_, err = c.nc.Write(o.frame)
		}
		if tcp, ok := c.raw.(*net.TCPConn); ok && err != nil {
			// The reply cannot be delivered: keep no unsent output for the connection.
			_ = tcp.SetLinger(0)
		}
		c.written(o)
		if err == nil && o.fatal {
			c.linger()
		}
		if err != nil || o.fatal {
			c.close()
			return
		}
	}
}

// written gives back what a reply held.
func (c *conn) written(o outgoing) {
	if o.budget {
		c.unreserve()
	}
	if o.size == 0 {
		return
	}
	c.mu.Lock()
	delete(c.ids, o.id)
	c.count--
	c.bytes -= o.size
	if c.count == 0 {
		c.idle()
	}
	c.mu.Unlock()
	signal(c.freed)
}

// linger lets a connection error reach the client: closing with unread input can reset the connection and lose it.
func (c *conn) linger() {
	half, ok := c.nc.(interface{ CloseWrite() error })
	if !ok || half.CloseWrite() != nil {
		return
	}
	if c.nc.SetReadDeadline(time.Now().Add(time.Second)) == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(c.nc, maxFrame))
	}
}

// close ends the connection once: waiting reads stop and unsent replies are dropped.
func (c *conn) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	dropped := c.queue
	c.queue = nil
	c.mu.Unlock()
	c.cancel()
	for _, o := range dropped {
		if o.budget {
			c.unreserve()
		}
	}
	_ = c.nc.SetDeadline(time.Now().Add(time.Second))
	_ = c.nc.Close()
}
