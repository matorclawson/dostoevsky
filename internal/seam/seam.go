// Package seam is where tests of this module replace the clock and the source of randomness that reach stored bytes.
// It is internal, so programs that import Dostoevsky cannot reach it. Until Fix is called, both are the standard
// library's own functions.
//
// Fix exists so that two runs, and two implementations, given the same requests one at a time write the same bytes:
//
//   - the k-th reading of the clock after Fix (k from 0) is start + k*step nanoseconds since the Unix epoch, plus
//     whatever Advance has added;
//   - each request for n random bytes takes SHA-256(seed || counter) blocks, seed and counter as 8 bytes
//     little-endian, the counter starting at 0 after Fix and going up by one per block, and keeps the first n bytes.
//     What is left of the last block is not used again.
//
// Another implementation must read the clock and the randomness at the same moments, as often, as this one does.
package seam

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"time"
)

var (
	// Now is the clock for every time that is stored or compared with a stored time.
	Now = time.Now
	// Random fills a slice with the randomness that is stored: store generations and lease tokens.
	Random = rand.Read
)

type fixed struct {
	mu          sync.Mutex
	next, step  int64
	seed, count uint64
}

var current *fixed

// Fix replaces the clock and the randomness as described above. Call it before opening anything: it is not safe to call
// while a node or store is in use.
func Fix(start, step int64, seed uint64) {
	f := &fixed{next: start, step: step, seed: seed}
	current = f
	Now = func() time.Time {
		f.mu.Lock()
		defer f.mu.Unlock()
		t := f.next
		f.next += f.step
		return time.Unix(0, t).UTC()
	}
	Random = func(p []byte) (int, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var in [16]byte
		binary.LittleEndian.PutUint64(in[:8], f.seed)
		for done := 0; done < len(p); {
			binary.LittleEndian.PutUint64(in[8:], f.count)
			f.count++
			block := sha256.Sum256(in[:])
			done += copy(p[done:], block[:])
		}
		return len(p), nil
	}
}

// Advance moves the fixed clock forward. It does nothing when the clock is not fixed.
func Advance(d time.Duration) {
	if f := current; f != nil {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.next += int64(d)
	}
}

// Restore puts the standard library's clock and randomness back.
func Restore() {
	current, Now, Random = nil, time.Now, rand.Read
}
