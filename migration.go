package dostoevsky

import "context"

// MigrateSegmented creates a segmented copy of an open legacy node in a new directory.
// The source is locked during the copy. The destination follows Backup's publication contract.
func (n *Node) MigrateSegmented(ctx context.Context, dir string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	if n.j.failed != nil {
		return n.j.failed
	}
	if n.segments != nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return publishSnapshot(dir, func(stage string) error {
		dst, err := OpenSegmented(stage, n.cfg)
		if err != nil {
			return err
		}
		defer dst.Close()
		l := dst.segments
		for p, s := range n.streams {
			pid, _, err := l.state.ensure(p)
			if err != nil {
				return err
			}
			l.state.parts[pid].base = s.base
			l.state.parts[pid].next = s.base
		}
		roll := func() error { id := l.active.id + 1; return dst.segmentRoll(l.state.snapshot(id, id)) }
		if err = roll(); err != nil {
			return err
		}
		for p, s := range n.streams {
			pid := l.state.byPart[p]
			var frames []byte
			type pending struct {
				rec record3
				pos int
			}
			var batch []pending
			flush := func() error {
				if len(batch) == 0 {
					return nil
				}
				if err := dst.segmentAppend(frames, 0); err != nil {
					return err
				}
				start := l.active.end - int64(len(frames))
				for _, v := range batch {
					if err := l.state.applyRecord(pid, v.rec.Offset); err != nil {
						return err
					}
					l.observe(v.rec, start+int64(v.pos))
				}
				clear(batch)
				batch = batch[:0]
				frames = frames[:0]
				return nil
			}
			for i := int64(0); i < s.count(); i++ {
				if err = ctx.Err(); err != nil {
					return err
				}
				ev, err := n.j.read(s.location(i))
				if err != nil {
					return err
				}
				r := ev.Record
				if r == nil || r.Partition != p || r.Offset != s.base+i {
					return ErrCorruptLog
				}
				stamp, err := timeToNanos(r.Time)
				if err != nil {
					return err
				}
				rec := record3{Partition: pid, Offset: uint64(r.Offset), Time: stamp, Key: []byte(r.Key), ID: []byte(r.ID), Value: r.Value}
				at := len(frames)
				frames, err = appendRecord3(frames, rec)
				if err != nil {
					return err
				}
				rec.Key = nil
				rec.Value = nil
				batch = append(batch, pending{rec, at})
				if len(frames) >= 1<<20 || len(batch) >= 256 {
					if err = flush(); err != nil {
						return err
					}
				}
			}
			if err := flush(); err != nil {
				return err
			}
			l.state.parts[pid].groups = s.groups
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = roll(); err != nil {
			return err
		}
		return dst.Close()
	})
}
