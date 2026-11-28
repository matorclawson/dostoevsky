// Package workflow runs durable, sequential activity workflows in a dedicated
// segmented Dostoevsky directory. Open starts the owner and Close releases it.
package workflow

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	d "github.com/rodriveracom/dostoevsky"
)

const topic = "transitions"
const transitionLimit = 256 << 10

var ErrStartConflict = errors.New("run is bound to another start request")
var ErrStale = errors.New("stale activity lease")
var ErrMissing = errors.New("run does not exist")

type Step struct {
	Kind, Name    string
	Attempts      int
	BackoffMillis int64
}
type Task struct {
	Run, Activity, Token string
	Step, Attempt        int
	Input                json.RawMessage
}
type Event struct {
	Sequence, TimeMillis int64
	Kind                 string
	Step                 int
	Data                 string
}

type definition struct {
	Name    string
	Version int
}
type transition struct {
	Format            int
	Kind              string
	At                int64
	Run, Queue, Token string
	Definition        definition
	Steps             []Step
	Data              []byte // base64 in JSON, preserving the exact bytes used by StartOnce
	Attrs             map[string]string
	Due               int64
}
type run struct {
	start         transition
	value         []byte
	step, attempt int
	token         string
	due           int64
	index         int
	history       []Event
}
type dueQueue []*run

func (q dueQueue) Len() int { return len(q) }
func (q dueQueue) Less(i, j int) bool {
	if q[i].due == q[j].due {
		return q[i].start.Run < q[j].start.Run
	}
	return q[i].due < q[j].due
}
func (q dueQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i]; q[i].index = i; q[j].index = j }
func (q *dueQueue) Push(x any)   { r := x.(*run); r.index = len(*q); *q = append(*q, r) }
func (q *dueQueue) Pop() any {
	a := *q
	r := a[len(a)-1]
	a[len(a)-1] = nil
	*q = a[:len(a)-1]
	r.index = -1
	return r
}

type command struct {
	act  func(*Store) error
	done chan error
}
type Store struct {
	node        *d.WorkflowLog
	calls       chan command
	stopped     chan struct{}
	once        sync.Once
	closeErr    error
	defs        map[definition][]Step
	runs        map[string]*run
	queues      map[string]*dueQueue
	seq         int64
	pending     []d.Publication
	appendGroup func(context.Context, []d.Publication) ([]d.Record, error)
	// Internal test hooks only. Set before submitting commands.
	afterSync func()
	now       func() time.Time
}

func Open(dir string) (*Store, error) {
	n, e := d.OpenWorkflowLog(dir)
	if e != nil {
		return nil, e
	}
	s := &Store{node: n, calls: make(chan command, 16), stopped: make(chan struct{}), defs: make(map[definition][]Step), runs: make(map[string]*run), queues: make(map[string]*dueQueue), now: time.Now, appendGroup: n.PublishBatch}
	for off := int64(0); ; {
		page, err := n.ReadBatch(context.Background(), d.Partition{Topic: topic}, off, d.Limits{Records: 128, Bytes: 1 << 20}, false)
		if errors.Is(err, d.ErrEmpty) {
			break
		}
		if err != nil {
			n.Close()
			return nil, err
		}
		for _, r := range page.Records {
			var tr transition
			if err = json.Unmarshal(r.Value, &tr); err == nil {
				err = s.apply(tr)
			}
			if err != nil {
				n.Close()
				return nil, fmt.Errorf("workflow transition %d: %w", r.Offset, err)
			}
		}
		off = page.Next
	}
	go s.serve()
	return s, nil
}
func (s *Store) Close() error {
	s.once.Do(func() { s.call(nil); <-s.stopped; s.closeErr = s.node.Close() })
	return s.closeErr
}
func (s *Store) call(f func(*Store) error) error {
	c := command{f, make(chan error, 1)}
	select {
	case <-s.stopped:
		return d.ErrClosed
	case s.calls <- c:
	}
	select {
	case e := <-c.done:
		return e
	case <-s.stopped:
		select {
		case e := <-c.done:
			return e
		default:
			return d.ErrClosed
		}
	}
}

func (s *Store) serve() {
	defer close(s.stopped)
	var fenced error
	for {
		// Drain only commands already waiting. Reserve one maximum-sized
		// command before admitting it. A Poll can emit at most 32 small
		// expiry/lease records, every other command emits at most one.
		group := [32]command{<-s.calls}
		available := min(len(group), 1+len(s.calls))
		var result [32]error
		closing := false
		n, encoded := 0, 0
		for n < available && encoded < 3<<20 && len(s.pending) <= 1024-32 {
			if n > 0 {
				group[n] = <-s.calls
			}
			c := group[n]
			n++
			if c.act == nil {
				closing = true
				break
			}
			if fenced == nil {
				before := len(s.pending)
				result[n-1] = c.act(s)
				for _, item := range s.pending[before:] {
					encoded += len(item.Message.Value) + 128
				}
			}
		}
		if fenced == nil && len(s.pending) > 0 {
			_, fenced = s.appendGroup(context.Background(), s.pending)
			if fenced == nil && s.afterSync != nil {
				s.afterSync()
			}
		}
		clear(s.pending)
		s.pending = s.pending[:0]
		for i, c := range group[:n] {
			if fenced != nil {
				result[i] = fenced
			}
			c.done <- result[i]
		}
		if closing {
			return
		}
	}
}

func (s *Store) emit(t transition) error {
	t.Format = 1
	b, e := json.Marshal(t)
	if e != nil {
		return e
	}
	if len(b) > transitionLimit {
		return d.ErrLimit
	}
	if e = s.apply(t); e != nil {
		return e
	}
	s.pending = append(s.pending, d.Publication{Topic: topic, Message: d.Message{Value: b}})
	return nil
}
func (s *Store) event(r *run, t transition, kind, data string, step int) {
	s.seq++
	r.history = append(r.history, Event{s.seq, t.At, kind, step, data})
}
func (s *Store) schedule(r *run, due int64) {
	q := s.queues[r.start.Queue]
	if q == nil {
		q = new(dueQueue)
		s.queues[r.start.Queue] = q
	}
	r.due = due
	if r.index < 0 {
		heap.Push(q, r)
	} else {
		heap.Fix(q, r.index)
	}
}
func (s *Store) remove(r *run) {
	q := s.queues[r.start.Queue]
	heap.Remove(q, r.index)
	if q.Len() == 0 {
		delete(s.queues, r.start.Queue)
	}
}
func (s *Store) advance(r *run, t transition) {
	steps := s.defs[r.start.Definition]
	r.token = ""
	r.attempt = 0
	if r.step == len(steps) {
		s.event(r, t, "WorkflowCompleted", string(r.value), r.step)
		if r.index >= 0 {
			s.remove(r)
		}
		return
	}
	b, _ := json.Marshal(map[string]any{"name": steps[r.step].Name, "due": t.At})
	s.event(r, t, "ActivityScheduled", string(b), r.step)
	s.schedule(r, t.At)
}

func (s *Store) apply(t transition) error {
	if t.Format != 1 {
		return errors.New("unknown workflow format")
	}
	r := s.runs[t.Run]
	switch t.Kind {
	case "register":
		if len(t.Steps) == 0 || s.defs[t.Definition] != nil {
			return d.ErrInvalid
		}
		s.defs[t.Definition] = t.Steps
	case "start":
		if r != nil || len(s.defs[t.Definition]) == 0 {
			return d.ErrInvalid
		}
		r = &run{start: t, value: t.Data, index: -1}
		s.runs[t.Run] = r
		s.event(r, t, "WorkflowStarted", string(t.Data), 0)
		if len(t.Attrs) > 0 {
			b, _ := json.Marshal(t.Attrs)
			s.event(r, t, "AttributesUpdated", string(b), -1)
		}
		s.advance(r, t)
	case "lease":
		if r == nil || r.index < 0 || r.token != "" || t.Token == "" {
			return d.ErrInvalid
		}
		r.attempt++
		r.token = t.Token
		s.schedule(r, t.Due)
		s.event(r, t, "ActivityStarted", strconv.Itoa(r.attempt), r.step)
	case "finish", "failure", "timeout":
		if r == nil || r.index < 0 || r.token == "" || t.Token != r.token {
			return d.ErrInvalid
		}
		if t.Kind == "finish" {
			s.event(r, t, "ActivityCompleted", string(t.Data), r.step)
			r.value = t.Data
			r.step++
			s.advance(r, t)
			return nil
		}
		reason := string(t.Data)
		if t.Kind == "timeout" {
			s.event(r, t, "ActivityTimedOut", "", r.step)
			reason = "lease expired"
		} else {
			s.event(r, t, "ActivityFailed", reason, r.step)
		}
		r.token = ""
		if r.attempt >= max(1, s.defs[r.start.Definition][r.step].Attempts) {
			s.event(r, t, "WorkflowFailed", reason, r.step)
			s.remove(r)
		} else {
			s.event(r, t, "RetryScheduled", reason, r.step)
			s.schedule(r, t.Due)
		}
	default:
		return d.ErrInvalid
	}
	return nil
}
func label(v string) bool { return len(v) > 0 && len(v) <= 256 && utf8.ValidString(v) }
func (s *Store) Register(name string, version int, steps []Step) error {
	if !label(name) || version < 1 || len(steps) == 0 || len(steps) > 256 {
		return d.ErrInvalid
	}
	for _, v := range steps {
		if v.Kind != "activity" || !label(v.Name) || v.Attempts < 0 || v.BackoffMillis < 0 || v.BackoffMillis > 1<<45 {
			return d.ErrInvalid
		}
	}
	key := definition{name, version}
	return s.call(func(s *Store) error {
		if prev := s.defs[key]; prev != nil {
			if !equalSteps(prev, steps) {
				return d.ErrConflict
			}
			return nil
		}
		return s.emit(transition{Kind: "register", At: s.now().UnixMilli(), Definition: key, Steps: append([]Step(nil), steps...)})
	})
}
func equalSteps(a, b []Step) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Store) StartOnce(id, name string, version int, queue string, input json.RawMessage, attrs map[string]string) (bool, error) {
	if !label(id) || !label(name) || !label(queue) || version < 1 || len(input) > transitionLimit || !json.Valid(input) {
		return false, d.ErrInvalid
	}
	size := 0
	for k, v := range attrs {
		if !label(k) || !utf8.ValidString(v) {
			return false, d.ErrInvalid
		}
		size += len(k) + len(v)
		if size > transitionLimit {
			return false, d.ErrLimit
		}
	}
	def := definition{name, version}
	created := false
	err := s.call(func(s *Store) error {
		if r := s.runs[id]; r != nil {
			a := r.start
			if a.Definition != def || a.Queue != queue || !bytes.Equal(a.Data, input) || len(a.Attrs) != len(attrs) {
				return ErrStartConflict
			}
			for k, v := range attrs {
				old, ok := a.Attrs[k]
				if !ok || old != v {
					return ErrStartConflict
				}
			}
			return nil
		}
		if s.defs[def] == nil {
			return d.ErrInvalid
		}
		copied := make(map[string]string, len(attrs))
		for k, v := range attrs {
			copied[k] = v
		}
		err := s.emit(transition{Kind: "start", At: s.now().UnixMilli(), Run: id, Definition: def, Queue: queue, Data: bytes.Clone(input), Attrs: copied})
		created = err == nil
		return err
	})
	return err == nil && created, err
}

func (s *Store) Poll(queue string, lease time.Duration) (*Task, error) {
	if !label(queue) || lease.Milliseconds() < 1 {
		return nil, d.ErrInvalid
	}
	var task *Task
	err := s.call(func(s *Store) error {
		var token string
		for i := 0; i < 32; i++ {
			now := s.now().UnixMilli()
			q := s.queues[queue]
			if q == nil || (*q)[0].due > now {
				return nil
			}
			// Acquire entropy before staging any expirations in this Poll.
			if token == "" {
				var b [16]byte
				if _, err := rand.Read(b[:]); err != nil {
					return err
				}
				token = hex.EncodeToString(b[:])
			}
			r := (*q)[0]
			step := s.defs[r.start.Definition][r.step]
			if r.token != "" {
				err := s.emit(transition{Kind: "timeout", Run: r.start.Run, At: now, Due: now + step.BackoffMillis, Token: r.token})
				if err != nil {
					return err
				}
				continue
			}
			err := s.emit(transition{Kind: "lease", Run: r.start.Run, At: now, Due: now + lease.Milliseconds(), Token: token})
			if err == nil {
				task = &Task{r.start.Run, step.Name, token, r.step, r.attempt, bytes.Clone(r.value)}
			}
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return task, nil
}
func (s *Store) Finish(id, token string, value json.RawMessage, failure string) error {
	if len(value) > transitionLimit || len(failure) > transitionLimit {
		return d.ErrLimit
	}
	return s.call(func(s *Store) error {
		r := s.runs[id]
		if r == nil {
			return ErrMissing
		}
		now := s.now().UnixMilli()
		if token == "" || r.token != token || r.due <= now {
			return ErrStale
		}
		t := transition{Kind: "finish", At: now, Run: id, Token: token, Data: bytes.Clone(value)}
		if failure != "" {
			t.Kind = "failure"
			t.Data = []byte(failure)
			t.Due = now + s.defs[r.start.Definition][r.step].BackoffMillis
		} else if !json.Valid(value) {
			return d.ErrInvalid
		}
		return s.emit(t)
	})
}
func (s *Store) History(id string, after int64, limit int) ([]Event, error) {
	if limit < 1 || limit > 1000 {
		return nil, d.ErrInvalid
	}
	out := []Event{}
	err := s.call(func(s *Store) error {
		if r := s.runs[id]; r != nil {
			i := sort.Search(len(r.history), func(i int) bool { return r.history[i].Sequence > after })
			out = append(out, r.history[i:i+min(limit, len(r.history)-i)]...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
