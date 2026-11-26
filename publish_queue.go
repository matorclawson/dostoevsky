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
	// After a group is acknowledged, the drainer waits for the callers it just released
	// to queue their next calls before it takes the next group (see gather). For the
	// first gatherLimit it spins; a longer wait, up to half the last write and never
	// more than maxGather, sleeps until the callers arrive. A lone caller never waits.
	gatherLimit = 100 * time.Microsecond
	maxGather   = 2 * time.Millisecond
	// After long waits that ran out, this many doublings of groups skip the long wait.
	maxGatherMisses = 6
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
	// now and yield are the clock and the scheduler yield gather uses. They are nil in
	// production (time.Now and runtime.Gosched); a test sets them on its own queue to
	// drive gather without wall-clock timing. They are per queue, not package globals,
	// so a drainer left over from another test can never race with the swap.
	now   func() time.Time
	yield func()
	// want is the queue length a sleeping drainer is waiting for, or zero; ready wakes
	// it. Both are guarded by mu.
	want  int
	ready chan struct{}
	// misses and skip back the long wait off when callers do not come back. Only the
	// one drainer touches them. cap replaces maxGather in tests.
	misses, skip int
	cap          time.Duration
}

func (q *publishQueue) clock() time.Time {
	if q.now != nil {
		return q.now()
	}
	return time.Now()
}

func (q *publishQueue) relax() {
	if q.yield != nil {
		q.yield()
		return
	}
	runtime.Gosched()
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
		q.wake() // the queue cannot grow: waiting longer would gain nothing
		q.mu.Unlock()
		n.mu.Lock()
		defer n.mu.Unlock()
		q.note(1)
		return n.publishLocked(ctx, p, m)
	}
	w := &queuedPublish{ctx: ctx, partition: p, msg: m, done: make(chan struct{})}
	q.pending = append(q.pending, w)
	q.bytes += size
	if q.want > 0 && len(q.pending) >= q.want {
		q.wake()
	}
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
		began := n.publishes.clock()
		n.writeQueued(batch)
		wrote := n.publishes.clock().Sub(began)
		n.mu.Unlock()
		released := len(batch)
		// Counted before anyone is released: these queued while this group was being
		// written, and the next group should hold them AND the released callers.
		n.publishes.mu.Lock()
		waiting := len(n.publishes.pending)
		n.publishes.mu.Unlock()
		for _, w := range batch {
			close(w.done)
		}
		n.publishes.gather(released, waiting, wrote)
		batch = n.publishes.take()
	}
}

// gather gives the callers that were just acknowledged a moment to come back with
// their next calls. With many synchronous producers every caller of a group is
// waiting for that group; taking the queue the instant they are released would write
// the others alone and the released ones in the group after, so each caller would be
// acknowledged only every second sync. The next group should therefore hold the
// callers that queued during the write (waiting) and the released ones: counting only
// as many as were released would be satisfied by the waiting half of a split at once,
// and the two halves would alternate for ever.
//
// Over a network the callers take longer than gatherLimit to return, so after
// spinning that long the drainer sleeps until they arrive, for at most half of what
// the last write took (a slow disk makes a missed group costly, a fast one does not)
// and at most maxGather. If they do not arrive, as with publishers that call once,
// the long wait is skipped for a doubling number of groups. A lone caller never waits,
// and no wait delays an acknowledgment: it happens after the group is released.
func (q *publishQueue) gather(released, waiting int, wrote time.Duration) {
	target := min(released+waiting, maxQueuedPublishes)
	if target < 2 {
		return
	}
	began := q.clock()
	spun := began.Add(gatherLimit)
	for {
		q.mu.Lock()
		pending := len(q.pending)
		q.mu.Unlock()
		if pending >= target {
			q.misses, q.skip = 0, 0
			return
		}
		if !q.clock().Before(spun) {
			break
		}
		q.relax()
	}
	limit := maxGather
	if q.cap > 0 {
		limit = q.cap
	}
	limit = min(wrote/2, limit)
	if limit <= gatherLimit {
		return
	}
	if q.skip > 0 {
		q.skip--
		return
	}
	q.mu.Lock()
	if len(q.pending) >= target {
		q.mu.Unlock()
		q.misses = 0
		return
	}
	if q.ready == nil {
		q.ready = make(chan struct{}, 1)
	}
	select { // a wake left over from an earlier wait
	case <-q.ready:
	default:
	}
	q.want = target
	q.mu.Unlock()
	timer := time.NewTimer(limit - gatherLimit)
	defer timer.Stop()
	select {
	case <-q.ready:
		q.misses = 0
	case <-timer.C:
		q.misses = min(q.misses+1, maxGatherMisses)
		q.skip = 1<<q.misses - 1
	}
	q.mu.Lock()
	q.want = 0
	q.mu.Unlock()
}

// wake ends a drainer's sleep, if there is one. The caller holds mu.
func (q *publishQueue) wake() {
	if q.want == 0 {
		return
	}
	q.want = 0
	select {
	case q.ready <- struct{}{}:
	default:
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
