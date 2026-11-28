package dostoevsky

import (
	"fmt"
	"math"
)

// The journal and the in-memory index grow with the history that is kept, and Open replays all of it. A size cap bounds
// all three: when the journal is at its cap, publishing is refused until Retain has made room (or the cap is raised).
//
// Consumer commits are allowed a further reserve, because Retain can only remove what every group has committed past:
// refusing commits as soon as publishes are refused would leave no way to make room.

// ErrJournalFull is returned for a write that would take the journal past its cap. It is also ErrLimit.
var ErrJournalFull = fmt.Errorf("%w: the journal is at its size cap", ErrLimit)

// minCommitReserve is the least room kept for commits above the cap (a commit's state may be up to MaxValue).
const minCommitReserve = 4 << 20

// SetJournalCap bounds the journal: publishing fails with ErrJournalFull rather than let it exceed maxBytes, and
// commits rather than let it exceed maxBytes plus a reserve of an eighth of maxBytes, at least 4 MiB. Zero, the default,
// is no cap. If the reserve is used up too (a group far behind, or large states), commits are refused as well: raise the
// cap, let the groups catch up, and Retain. The cap belongs to this Node, not to the directory: set it after every Open. A journal already past the cap
// opens and can be read, committed to within the reserve, and compacted with Retain.
func (n *Node) SetJournalCap(maxBytes int64) error {
	if maxBytes < 0 {
		return ErrInvalid
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	n.maxBytes = maxBytes
	return nil
}

// JournalSize reports physical journal bytes, including checkpoints in segmented stores.
// Derived indexes, lock files and directory metadata are excluded.
func (n *Node) JournalSize() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.segments != nil {
		return n.segments.bytes
	}
	return n.j.end
}

// commitCeiling is where commits stop; zero when there is no cap. The caller holds mu.
func (n *Node) commitCeiling() int64 {
	if n.maxBytes == 0 {
		return 0
	}
	reserve := max(n.maxBytes/8, minCommitReserve)
	if n.maxBytes > math.MaxInt64-reserve { // an overflowed ceiling would be negative, which means none
		return math.MaxInt64
	}
	return n.maxBytes + reserve
}
