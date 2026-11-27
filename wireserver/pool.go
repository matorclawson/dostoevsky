package wireserver

import (
	"slices"
	"sync"
)

// pool is a byte budget. Waiters are served in arrival order, so a large request is not starved by small ones.
type pool struct {
	mu      sync.Mutex
	free    int
	waiting []*waiter
}

type waiter struct {
	n     int
	ready chan struct{}
}

// take waits for n bytes. It gives up, holding nothing, when done is closed. A nil done never closes.
func (p *pool) take(n int, done <-chan struct{}) bool {
	p.mu.Lock()
	if len(p.waiting) == 0 && n <= p.free {
		p.free -= n
		p.mu.Unlock()
		return true
	}
	w := &waiter{n: n, ready: make(chan struct{})}
	p.waiting = append(p.waiting, w)
	p.mu.Unlock()
	select {
	case <-w.ready:
		return true
	case <-done:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-w.ready:
		p.free += n
	default:
		p.waiting = slices.DeleteFunc(p.waiting, func(o *waiter) bool { return o == w })
	}
	p.grant()
	return false
}

func (p *pool) give(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.free += n
	p.grant()
}

// grant is called with mu held.
func (p *pool) grant() {
	for len(p.waiting) > 0 && p.waiting[0].n <= p.free {
		w := p.waiting[0]
		p.waiting = p.waiting[1:]
		p.free -= w.n
		close(w.ready)
	}
}
