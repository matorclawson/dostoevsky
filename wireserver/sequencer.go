package wireserver

import (
	"context"
	"errors"
	"sync"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/wire"
)

// A group is bounded like the node's own publish queue. One publish above the byte bound goes alone.
const (
	maxGroup      = 64
	maxGroupBytes = 1 << 20
)

type write struct {
	c *conn
	r request
}

// sequencer applies every PUBLISH and COMMIT of the server in arrival order, which keeps each connection's order.
// Publishes that queued up while the disk was busy go to the store as one batch, under one sync. What is queued is
// bounded by the request pool, whose bytes each write holds until it is durable or has failed.
type sequencer struct {
	mu      sync.Mutex
	more    sync.Cond
	pending []write
	stop    bool
	done    chan struct{}
}

func (q *sequencer) start(store Store) {
	q.more.L = &q.mu
	q.done = make(chan struct{})
	go q.run(store)
}

func (q *sequencer) add(w write) {
	q.mu.Lock()
	q.pending = append(q.pending, w)
	q.mu.Unlock()
	q.more.Signal()
}

// close runs what is queued and stops. No add may follow it.
func (q *sequencer) close() {
	q.mu.Lock()
	q.stop = true
	q.mu.Unlock()
	q.more.Signal()
	<-q.done
}

// take returns one commit, or the run of publishes at the head of the queue. It returns nil when stopped and empty.
func (q *sequencer) take() []write {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.pending) == 0 {
		if q.stop {
			return nil
		}
		q.more.Wait()
	}
	n, bytes := 1, q.pending[0].r.size
	if q.pending[0].r.q.Type == wire.Publish {
		for n < len(q.pending) && n < maxGroup {
			next := q.pending[n].r
			if next.q.Type != wire.Publish || bytes+next.size > maxGroupBytes {
				break
			}
			bytes += next.size
			n++
		}
	}
	group := append([]write(nil), q.pending[:n]...)
	clear(q.pending[:n])
	q.pending = q.pending[n:]
	return group
}

func (q *sequencer) run(store Store) {
	defer close(q.done)
	for group := q.take(); group != nil; group = q.take() {
		apply(store, group)
	}
}

func publication(r request) d.Publication {
	return d.Publication{Topic: r.q.Topic, Message: d.Message{Key: r.q.Key, ID: r.q.MessageID, Value: r.q.Value}}
}

// apply writes a group. The writes run to completion even if their connections have closed.
func apply(store Store, group []write) {
	ctx := context.Background()
	if first := group[0]; first.r.q.Type == wire.Commit {
		q := first.r.q
		err := store.Commit(ctx, partition(q), q.Group, int64(q.Offset), q.Value)
		first.finish(wire.Reply{Type: wire.CommitOK, RequestID: q.RequestID}, err)
		return
	}
	if len(group) > 1 {
		items := make([]d.Publication, len(group))
		for i, w := range group {
			items[i] = publication(w.r)
		}
		records, err := store.PublishBatch(ctx, items)
		// The batch is validated before anything is written, so after one of these each publish is resolved on its
		// own: one conflicting retry must not fail another sender's publish.
		alone := errors.Is(err, d.ErrOwner) || errors.Is(err, d.ErrConflict) || errors.Is(err, d.ErrInvalid) || errors.Is(err, d.ErrLimit)
		if !alone {
			for i, w := range group {
				var rec d.Record
				if err == nil {
					rec = records[i]
				}
				w.published(rec, err)
			}
			return
		}
	}
	for _, w := range group {
		item := publication(w.r)
		rec, err := store.Publish(ctx, item.Topic, item.Message)
		w.published(rec, err)
	}
}

func (w write) published(rec d.Record, err error) {
	w.finish(wire.Reply{Type: wire.PublishOK, RequestID: w.r.q.RequestID, Partition: uint16(rec.Number), Offset: uint64(rec.Offset)}, err)
}

// finish gives back the frame's bytes and queues the reply.
func (w write) finish(ok wire.Reply, err error) {
	w.c.s.requests.give(w.r.size)
	if err != nil {
		w.c.refuse(w.r, err)
		return
	}
	w.c.answer(ok, w.r.size, false)
}
