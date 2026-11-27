package dostoevsky

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const segmentMarker = "DOSTOEVSKY-SEGMENTED-01\n"
const segmentTarget = 8 << 20

func (n *Node) openSegments(dir string, raw []byte, requested bool) (bool, error) {
	var b [len(segmentMarker)]byte
	k, err := n.j.file.ReadAt(b[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	marked := string(b[:k]) == segmentMarker
	path := filepath.Join(dir, "segments")
	if !marked && k != 0 {
		if k < len(segmentMarker) {
			if _, err := os.Stat(path); err == nil {
				return false, fmt.Errorf("%w: bad format marker", ErrCorruptLog)
			}
		}
		if requested {
			return false, fmt.Errorf("%w: existing journal requires migration", ErrInvalid)
		}
		return false, nil
	}
	if !marked {
		if _, e := os.Lstat(path); e == nil {
			return false, errors.New("unmarked segmented directory; refusing initialization")
		} else if !errors.Is(e, os.ErrNotExist) {
			return false, e
		}
		if !requested {
			return false, nil
		}
	}
	if marked {
		if err := checkSegmentRange(path); err != nil {
			return false, err
		}
	}
	log, err := openSegLog(osFS{}, path)
	if err != nil {
		return false, err
	}
	ok := false
	defer func() {
		if !ok {
			log.close()
		}
	}()
	if marked {
		if log.active == nil {
			return false, errors.New("segmented store has no active segment")
		}
		var saved Config
		if err = json.Unmarshal(log.state.config, &saved); err != nil {
			return false, err
		}
		if saved.Node != n.cfg.Node || !slices.Equal(saved.Nodes, n.cfg.Nodes) {
			return false, ErrInvalid
		}
		for t, count := range saved.Topics {
			if n.cfg.Topics[t] != count {
				return false, ErrInvalid
			}
		}
		if len(saved.Topics) != len(n.cfg.Topics) {
			log.state.setConfig(raw)
			id := log.active.id + 1
			if err = log.roll(log.state.snapshot(id, id)); err != nil {
				return false, err
			}
		}
	} else {
		log.state.setConfig(raw)
		if err = log.start(log.state.snapshot(1, 1)); err != nil {
			return false, err
		}
		if err = writeSegmentRange(log, 1); err != nil {
			return false, err
		}
		if err = syncDir(dir); err != nil {
			return false, err
		}
		if _, err = n.j.file.WriteAt([]byte(segmentMarker), 0); err != nil {
			return false, err
		}
		if err = n.j.file.Sync(); err != nil {
			return false, err
		}
	}
	if marked {
		first := log.active.id
		if len(log.ids) > 0 {
			first = log.ids[0]
		}
		if err = ensureSegmentRange(log, first); err != nil {
			return false, err
		}
	}
	log.bytes = log.active.end
	for _, id := range log.ids {
		info, e := os.Stat(filepath.Join(path, segName(id)))
		if e != nil {
			return false, e
		}
		log.bytes += info.Size()
	}
	if log.active.torn.Bytes > 0 {
		n.truncated = log.active.torn
	}
	log.filters = &idFilterCache{entries: make(map[uint64]idFilter)}
	n.segments = log
	n.rollBytes = segmentTarget
	ok = true
	return true, nil
}

func (n *Node) segmentRecord(p Partition, offset int64) (Record, error) {
	l := n.segments
	r, e := l.readRecord(l.state.byPart[p], uint64(offset))
	if e != nil {
		return Record{}, e
	}
	return Record{Partition: p, Offset: int64(r.Offset), Time: nanosToTime(r.Time), Message: Message{Key: string(r.Key), ID: string(r.ID), Value: r.Value}}, nil
}

func (n *Node) segmentPrior(p Partition, id string) (*Record, error) {
	l := n.segments
	pid, ok := l.state.byPart[p]
	if !ok {
		return nil, nil
	}
	var found *Record
	var readErr error
	err := l.findIDs(pid, []byte(id), len(l.ids), func(segment, off uint64) bool {
		if off < uint64(l.state.parts[pid].base) {
			return false
		}
		index := l.idx
		if segment != l.active.id {
			var err error
			index, err = l.sealedIndex(segment)
			if err != nil {
				readErr = err
				return true
			}
		}
		pos, ok := index.find(pid, off)
		if !ok {
			readErr = ErrCorruptLog
			return true
		}
		rec, e := l.readLocatedRecord(segment, pos, pid, off)
		if e != nil {
			readErr = e
			return true
		}
		if string(rec.ID) == id {
			found = &Record{Partition: p, Offset: int64(off), Time: nanosToTime(rec.Time), Message: Message{Key: string(rec.Key), ID: id, Value: rec.Value}}
			return true
		}
		return false
	})
	return found, errors.Join(err, readErr)
}

func (n *Node) segmentRoll(cp checkpoint3) error {
	l := n.segments
	if err := l.roll(cp); err != nil {
		return err
	}
	if err := writeSegmentRange(l, l.ids[0]); err != nil {
		l.failed = err
		return err
	}
	l.bytes += l.active.end
	return nil
}

func (n *Node) segmentAppend(frames []byte, ceiling int64) error {
	l := n.segments
	if l.failed != nil {
		return l.failed
	}
	// A catalog checkpoint may exceed the segment target. Count only the appended tail.
	if l.active.end-l.active.dataStart >= n.rollBytes {
		id := l.active.id + 1
		cp := l.state.snapshot(id, id)
		if ceiling > 0 {
			encoded, e := appendCheckpoint3(nil, cp)
			if e != nil {
				return e
			}
			if int64(len(encoded)+segHeaderSize+len(frames)) > ceiling-l.bytes {
				return ErrJournalFull
			}
		}
		if err := n.segmentRoll(cp); err != nil {
			return err
		}
	}
	if ceiling > 0 && int64(len(frames)) > ceiling-l.bytes {
		return ErrJournalFull
	}
	if err := l.append(frames); err != nil {
		return err
	}
	l.bytes += int64(len(frames))
	return nil
}

func (n *Node) segmentBatch(ctx context.Context, items []Publication) ([]Record, error) {
	l := n.segments
	if l.failed != nil {
		return nil, l.failed
	}
	type identity struct {
		p  Partition
		id string
	}
	seen := make(map[identity]int)
	type step struct {
		i, at int
		pid   uint32
	}
	var steps []step
	pids := make(map[Partition]uint32)
	counts := make(map[Partition]int64)
	nextPID := l.state.nextPID
	out := make([]Record, len(items))
	var buf []byte
	var inputBytes int
	for i, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m := item.Message
		inputBytes += len(m.Value) + len(m.Key) + len(m.ID) + len(item.Topic)
		if inputBytes > maxFrame {
			return nil, ErrLimit
		}
		if len(m.Value) > MaxValue || len(m.Key) > 4096 || len(m.ID) > 256 {
			return nil, ErrLimit
		}
		if !validText(m.Key, m.ID) {
			return nil, ErrInvalid
		}
		p, e := n.partition(item.Topic, m.Key)
		if e != nil {
			return nil, e
		}
		owner, e := n.owner(p)
		if e != nil {
			return nil, e
		}
		if owner != n.cfg.Node {
			return nil, ErrOwner
		}
		if m.ID != "" {
			key := identity{p, m.ID}
			var prior *Record
			if j, yes := seen[key]; yes {
				prior = &out[j]
			} else {
				prior, e = n.segmentPrior(p, m.ID)
				if e != nil {
					return nil, e
				}
			}
			if prior != nil {
				if prior.Key != m.Key || !bytes.Equal(prior.Value, m.Value) {
					return nil, ErrConflict
				}
				out[i] = *prior
				continue
			}
			seen[key] = i
		}
		pid, exists := l.state.byPart[p]
		var base int64
		if exists {
			base = l.state.parts[pid].next
		} else if pid, exists = pids[p]; !exists {
			if nextPID == ^uint32(0) {
				return nil, ErrLimit
			}
			pid = nextPID
			nextPID++
			pids[p] = pid
			buf, e = appendTopicDef3(buf, topicDef3{pid, uint32(p.Number), []byte(p.Topic)})
			if e != nil {
				return nil, e
			}
		}
		r := Record{Partition: p, Message: m, Time: time.Now().UTC(), Offset: base + counts[p]}
		counts[p]++
		steps = append(steps, step{i, len(buf), pid})
		out[i] = r
		buf, e = appendRecord3(buf, record3{Partition: pid, Offset: uint64(r.Offset), Time: r.Time.UnixNano(), Key: []byte(m.Key), ID: []byte(m.ID), Value: m.Value})
		if e != nil {
			return nil, e
		}
		if len(buf) > maxFrame {
			return nil, ErrLimit
		}
	}
	if len(buf) == 0 {
		return out, nil
	}
	if e := n.segmentAppend(buf, n.maxBytes); e != nil {
		return nil, e
	}
	for p, pid := range pids {
		if err := l.state.define(pid, p); err != nil {
			l.failed = err
			return nil, err
		}
	}
	start := l.active.end - int64(len(buf))
	for _, s := range steps {
		r := out[s.i]
		ps := l.state.parts[s.pid]
		ps.next++
		if waiting := n.streams[r.Partition]; waiting != nil {
			waiting.segmentState = ps
			waiting.signal()
		}
		l.observe(record3{Partition: s.pid, Offset: uint64(r.Offset), Time: r.Time.UnixNano(), ID: []byte(r.ID)}, start+int64(s.at))
	}
	return out, nil
}

func (n *Node) segmentCommit(s *stream, c checkpoint) error {
	l := n.segments
	pid := l.state.byPart[c.Partition]
	cmd := commit3{Partition: pid, Group: []byte(c.Group), Next: uint64(c.Next), State: c.State}
	if c.Expected != nil {
		cmd.HasExpected = true
		cmd.Expected = uint64(*c.Expected)
	}
	frame, e := appendCommit3(nil, cmd)
	if e != nil {
		return e
	}
	if e = n.segmentAppend(frame, n.commitCeiling()); e != nil {
		return e
	}
	if e = l.state.applyCommit(cmd); e != nil {
		l.failed = e
		return e
	}
	s.groups = l.state.parts[pid].groups
	return nil
}

func (n *Node) segmentRetain(floors map[Partition]int64) error {
	l := n.segments
	if l.failed != nil {
		return l.failed
	}
	changed := false
	for p, f := range floors {
		if base, _ := n.streamBounds(p); base != f {
			changed = true
			break
		}
	}
	if !changed && len(l.ids) == 0 {
		return nil
	}
	id := l.active.id + 1
	cp := l.state.snapshot(id, id)
	for i, part := range cp.Partitions {
		if f, ok := floors[l.state.byPID[part.Partition]]; ok {
			cp.Partitions[i].Base = uint64(f)
		}
	}
	if changed {
		if err := n.segmentRoll(cp); err != nil {
			return err
		}
	}
	for p, f := range floors {
		if pid, ok := l.state.byPart[p]; ok {
			l.state.parts[pid].base = f
		}
		if s := n.streams[p]; s != nil && s.segmentState != nil {
			s.base = f
			s.signal()
		}
	}
	// Removing only a contiguous prefix preserves the missing-segment corruption check.
	for len(l.ids) > 0 {
		old := l.ids[0]
		x, e := l.sealedIndex(old)
		if e != nil {
			return e
		}
		for pid, part := range x.parts {
			if part.first+part.count > uint64(l.state.parts[pid].base) {
				return nil
			}
		}
		path := filepath.Join(l.dir, segName(old))
		info, e := os.Stat(path)
		if e != nil {
			return e
		}
		if e = writeSegmentRange(l, old+1); e != nil {
			l.failed = e
			return e
		}
		if e = os.Remove(path); e != nil {
			return e
		}
		// Persist each prefix deletion before deleting its successor.
		if e = syncDir(l.dir); e != nil {
			l.failed = e
			return e
		}
		l.retire(old)
		l.bytes -= info.Size()
	}
	return nil
}

// The range file distinguishes intentional prefix retirement from lost segment files.
// Publish it after creating a segment and before unlinking an expired prefix.
func rangeBytes(first, last uint64) []byte {
	b := make([]byte, 20)
	binary.LittleEndian.PutUint64(b, first)
	binary.LittleEndian.PutUint64(b[8:], last)
	binary.LittleEndian.PutUint32(b[16:], crc32.Checksum(b[:16], crcTable))
	return b
}
func readSegmentRange(dir string) ([]byte, error) {
	f, e := os.Open(filepath.Join(dir, "RANGE"))
	if e != nil {
		return nil, e
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, 21))
}
func checkSegmentRange(dir string) error {
	b, err := readSegmentRange(dir)
	if err != nil {
		return fmt.Errorf("%w: segment range: %v", ErrCorruptLog, err)
	}
	if len(b) != 20 || crc32.Checksum(b[:16], crcTable) != binary.LittleEndian.Uint32(b[16:]) {
		return fmt.Errorf("%w: invalid segment range", ErrCorruptLog)
	}
	first, last := binary.LittleEndian.Uint64(b), binary.LittleEndian.Uint64(b[8:])
	if first == 0 || first > last {
		return ErrCorruptLog
	}
	for _, id := range []uint64{first, last} {
		if _, err := os.Stat(filepath.Join(dir, segName(id))); err != nil {
			return corrupt(id, 0, "required segment unavailable: %v", err)
		}
	}
	return nil
}
func ensureSegmentRange(l *segLog, first uint64) error {
	b, err := readSegmentRange(l.dir)
	if err != nil {
		return err
	}
	if bytes.Equal(b, rangeBytes(first, l.active.id)) {
		return nil
	}
	return writeSegmentRange(l, first)
}
func writeSegmentRange(l *segLog, first uint64) error {
	tmp := filepath.Join(l.dir, "RANGE.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(rangeBytes(first, l.active.id)); err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(l.dir, "RANGE"))
	}
	if err == nil {
		err = syncDir(l.dir)
	}
	return err
}

// readSegmentPage scans forward from a sparse index entry. Caller holds n.mu.RLock.
func (n *Node) readSegmentPage(ctx context.Context, p Partition, start int64, lim Limits) (Page, error) {
	page := Page{Next: start}
	if start < 0 {
		return Page{}, ErrInvalid
	}
	owner, e := n.owner(p)
	if e != nil {
		return Page{}, e
	}
	if owner != n.cfg.Node {
		return Page{}, ErrOwner
	}
	base, next := n.streamBounds(p)
	if start >= next {
		return Page{}, ErrEmpty
	}
	if start < base {
		return Page{}, ErrExpired
	}
	l := n.segments
	pid := l.state.byPart[p]
	for page.Next < next && len(page.Records) < lim.Records {
		if err := ctx.Err(); err != nil {
			return Page{}, err
		}
		id, pos, e := l.locate(pid, uint64(page.Next))
		if e != nil {
			return Page{}, e
		}
		nextBefore := page.Next
		done, e := func() (bool, error) {
			f, size := l.active.f, l.active.end
			if id != l.active.id {
				var err error
				f, err = l.fs.OpenFile(filepath.Join(l.dir, segName(id)), os.O_RDONLY, 0)
				if err != nil {
					return false, err
				}
				defer f.Close()
				size, err = f.Size()
				if err != nil {
					return false, err
				}
			}
			fr := frameReader{f: f, size: size, chunk: 64 << 10}
			for pos < size {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				typ, b, k, err := fr.next(pos)
				if err != nil {
					return false, corrupt(id, pos, "%v", err)
				}
				pos += int64(k)
				if typ != f3Record {
					continue
				}
				r, err := parseRecord3(b)
				if err != nil {
					return false, err
				}
				if r.Partition != pid || r.Offset < uint64(page.Next) {
					continue
				}
				if r.Offset != uint64(page.Next) {
					return false, corrupt(id, pos, "missing offset %d", page.Next)
				}
				cost := len(r.Key) + len(r.ID) + len(r.Value)
				if cost > lim.Bytes {
					if len(page.Records) == 0 {
						return false, ErrLimit
					}
					return true, nil
				}
				lim.Bytes -= cost
				page.Records = append(page.Records, Record{Partition: p, Offset: page.Next, Time: nanosToTime(r.Time), Message: Message{Key: string(r.Key), ID: string(r.ID), Value: bytes.Clone(r.Value)}})
				page.Next++
				if len(page.Records) == lim.Records {
					return true, nil
				}
			}
			return false, nil
		}()
		if e != nil {
			return Page{}, e
		}
		if done {
			break
		}
		if nextBefore == page.Next {
			return Page{}, corrupt(id, pos, "missing offset %d", page.Next)
		}
	}
	return page, nil
}
