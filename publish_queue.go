package dostoevsky

import (
	"bytes"
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// One isolated caller writes inline. Only callers that arrive during a write
// enqueue. Overflow falls back to the ordinary node lock for backpressure.
const (
	maxQueuedPublishes = 64
	maxQueuedBytes     = 1 << 20
	// After a group of two or more is acknowledged, the drainer waits at most this
	// long for the callers it just released to queue their next calls before it takes
	// the next group (see gather). A lone caller never waits.
	gatherLimit = 100 * time.Microsecond
)

// Hooks so tests can drive gather without wall-clock timing.
var (
	gatherYield = runtime.Gosched
	gatherNow   = time.Now
)

type queuedPublish struct {
	ctx       context.Context
	partition Partition
	msg       Message
	result    Record
	err       error
	done      chan struct{}
}
type publishQueue struct {
	mu      sync.Mutex
	active  bool
	pending []*queuedPublish
	bytes   int
	// groupAttempts and callAttempts count the journal writes attempted for Publish calls
	// (an inline write is a group of one) and the calls in them. They are bumped when a
	// write is attempted, before it succeeds or fails, so they are not a measure of
	// durable records. Tests read them to check how calls are batched.
	groupAttempts, callAttempts atomic.Int64
}

func (q *publishQueue) note(size int) {
	q.groupAttempts.Add(1)
	q.callAttempts.Add(int64(size))
}

func (n *Node) schedulePublish(ctx context.Context, p Partition, m Message) (Record, error) {
	q := &n.publishes
	q.mu.Lock()
	// An idle node writes inline, with no queue bookkeeping beyond this check. It must
	// not do so while a drainer is active: a call that wrote alone in the gap between two
	// groups would split the group the drainer is gathering.
	if !q.active && n.mu.TryLock() {
		q.mu.Unlock()
		defer n.mu.Unlock()
		q.note(1)
		return n.publishLocked(ctx, p, m)
	}
	if err := ctx.Err(); err != nil {
		q.mu.Unlock()
		return Record{}, err
	}
	if !q.active {
		q.active = true
		q.mu.Unlock()
		n.mu.Lock()
		q.note(1)
		r, err := n.publishLocked(ctx, p, m)
		n.mu.Unlock()
		if batch := q.take(); len(batch) > 0 {
			go n.drainPublishes(batch)
		}
		return r, err
	}
	size := len(p.Topic) + len(m.ID) + len(m.Key) + len(m.Value)
	if len(q.pending) >= maxQueuedPublishes || size > maxQueuedBytes-q.bytes {
		q.mu.Unlock()
		n.mu.Lock()
		defer n.mu.Unlock()
		q.note(1)
		return n.publishLocked(ctx, p, m)
	}
	w := &queuedPublish{ctx: ctx, partition: p, msg: m, done: make(chan struct{})}
	q.pending = append(q.pending, w)
	q.bytes += size
	q.mu.Unlock()
	<-w.done
	return w.result, w.err
}

func (q *publishQueue) take() []*queuedPublish {
	q.mu.Lock()
	defer q.mu.Unlock()
	batch := q.pending
	q.pending = nil
	q.bytes = 0
	q.active = len(batch) > 0
	return batch
}
func (n *Node) drainPublishes(batch []*queuedPublish) {
	for len(batch) > 0 {
		n.mu.Lock()
		n.writeQueued(batch)
		n.mu.Unlock()
		released := len(batch)
		for _, w := range batch {
			close(w.done)
		}
		n.publishes.gather(released)
		batch = n.publishes.take()
	}
}

// gather gives the callers that were just acknowledged a moment to come back with
// their next calls. With many synchronous producers every caller of a group is
// waiting for that group, so the queue is empty the instant they are released; taking
// it then ends the drain, and the next arrival writes alone while the rest pile up
// behind it, so groups alternate between one and many. Waiting until about as many
// calls are pending as were released (or gatherLimit, whichever comes first) keeps the
// groups full. Only groups of two or more wait, so a single producer sees no delay,
// and the wait never delays an acknowledgment: it happens after the group's callers
// have been released.
func (q *publishQueue) gather(released int) {
	if released < 2 {
		return
	}
	deadline := gatherNow().Add(gatherLimit)
	for {
		q.mu.Lock()
		pending := len(q.pending)
		q.mu.Unlock()
		if pending >= released || !gatherNow().Before(deadline) {
			return
		}
		gatherYield()
	}
}
func (n *Node) writeQueued(batch []*queuedPublish) {
	n.publishes.note(len(batch))
	if len(batch) > 1 {
		items := make([]Publication, 0, len(batch))
		for _, w := range batch {
			w.err = w.ctx.Err()
			if w.err == nil {
				items = append(items, Publication{Topic: w.partition.Topic, Message: w.msg})
			}
		}
		if len(items) == 0 {
			return
		}
		records, err := n.publishBatchLocked(context.Background(), items)
		switch err {
		case ErrOwner, ErrConflict, ErrInvalid, ErrLimit:
			// Batch validation precedes I/O. Resolve these calls independently so one
			// conflicting retry cannot reject another caller's valid publication.
		default:
			i := 0
			for _, w := range batch {
				if w.err != nil {
					continue
				}
				w.err = err
				if err == nil {
					w.result = records[i]
					// A deduplicated record may refer to another caller's input buffer.
					if w.msg.ID != "" {
						w.result.Value = bytes.Clone(w.result.Value)
					}
				}
				i++
			}
			return
		}
	}
	for _, w := range batch {
		w.result, w.err = n.publishLocked(w.ctx, w.partition, w.msg)
	}
}
