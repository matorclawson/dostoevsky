package dostoevsky

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// The segment file layer of the segmented log (design v1.1 and v1.2, change (b1)): a directory
// of segment files, each starting with a header and a checkpoint, written append-only, published
// by a staged create-sync-rename-sync, and scanned whole at Open. Nothing in the engine uses it
// yet. All file access goes through the fsys interface, so tests can run the whole layer on a
// model file system that crashes at any operation and then loses whatever was not synced.

// ErrCorruptLog is returned by Open when a committed segment fails validation. It is never
// returned for the result of a crash: a crash leaves, at worst, an unpublished staging file and a
// torn final append, both of which Open handles. Nothing is deleted or rewritten when it is
// returned; the offline Recover is the only repair path.
var ErrCorruptLog = errors.New("dostoevsky: the log is corrupt: a committed segment failed validation")

// errSegUncertain: a step after the point of no return failed (the directory sync of a roll), so
// it is unknown whether the new segment is durable. The log is fenced; reopening decides.
var errSegUncertain = errors.New("dostoevsky: the log fenced itself: durability of the last step is uncertain")

type fsFile interface {
	WriteAt(p []byte, off int64) (int, error)
	ReadAt(p []byte, off int64) (int, error)
	Sync() error     // data and metadata
	SyncData() error // data (and the size, when it grew): fdatasync
	Truncate(size int64) error
	Size() (int64, error)
	Close() error
}

type fsys interface {
	OpenFile(path string, flag int, perm os.FileMode) (fsFile, error)
	Rename(oldpath, newpath string) error
	Remove(path string) error
	ReadDir(dir string) ([]string, error) // names only
	SyncDir(dir string) error
	MkdirAll(dir string, perm os.FileMode) error
}

// ----- the real file system -----

type osFS struct{}

type osFile struct{ f *os.File }

func (osFS) OpenFile(path string, flag int, perm os.FileMode) (fsFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &osFile{f}, nil
}
func (osFS) Rename(a, b string) error               { return os.Rename(a, b) }
func (osFS) Remove(p string) error                  { return os.Remove(p) }
func (osFS) MkdirAll(d string, m os.FileMode) error { return os.MkdirAll(d, m) }
func (osFS) ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names, nil
}
func (osFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	return errors.Join(err, d.Close())
}

func (o *osFile) WriteAt(p []byte, off int64) (int, error) { return o.f.WriteAt(p, off) }
func (o *osFile) ReadAt(p []byte, off int64) (int, error)  { return o.f.ReadAt(p, off) }
func (o *osFile) Sync() error                              { return o.f.Sync() }
func (o *osFile) SyncData() error                          { return syncJournalData(o.f) }
func (o *osFile) Truncate(n int64) error                   { return o.f.Truncate(n) }
func (o *osFile) Close() error                             { return o.f.Close() }
func (o *osFile) Size() (int64, error) {
	fi, err := o.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// ----- segment files -----

const (
	segHeaderSize = 16
	segSignature  = "DOST0003"
	segVersion    = 3
)

func segName(id uint64) string    { return fmt.Sprintf("seg-%016x.log", id) }
func segTmpName(id uint64) string { return fmt.Sprintf("seg-%016x.tmp", id) }

func parseSegName(name string) (uint64, bool) {
	hex, ok := strings.CutPrefix(name, "seg-")
	if !ok {
		return 0, false
	}
	hex, ok = strings.CutSuffix(hex, ".log")
	if !ok || len(hex) != 16 {
		return 0, false
	}
	var id uint64
	for _, c := range []byte(hex) {
		var d byte
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'a' && c <= 'f':
			d = c - 'a' + 10
		default:
			return 0, false
		}
		id = id<<4 | uint64(d)
	}
	return id, id != 0
}

func segHeader() []byte {
	h := make([]byte, 0, segHeaderSize)
	h = append(h, segSignature...)
	h = binary.LittleEndian.AppendUint16(h, segVersion)
	h = binary.LittleEndian.AppendUint16(h, 0)
	return binary.LittleEndian.AppendUint32(h, crc32.Checksum(h, crcTable))
}

func checkSegHeader(h []byte) error {
	if len(h) < segHeaderSize || string(h[:8]) != segSignature {
		return errors.New("not a segment header")
	}
	if binary.LittleEndian.Uint32(h[12:]) != crc32.Checksum(h[:12], crcTable) {
		return errors.New("segment header check failed")
	}
	if v := binary.LittleEndian.Uint16(h[8:]); v != segVersion {
		return fmt.Errorf("segment format version %d, expected %d", v, segVersion)
	}
	if f := binary.LittleEndian.Uint16(h[10:]); f != 0 {
		return fmt.Errorf("segment flags %#x", f)
	}
	return nil
}

type segment struct {
	f   fsFile
	id  uint64
	end int64 // the logical end: where the next append goes
}

// createSegment publishes a new segment whose content is the header and the checkpoint cp
// (stamped with the segment's id). The order is the contract: stage as seg-<id>.tmp, write, fsync
// the file, rename to seg-<id>.log, fsync the directory; only then may anything be appended. A
// failure before the rename removes the staging file and leaves the log as it was; a failure of
// the directory sync returns errSegUncertain (the segment may or may not survive a crash, and
// nothing has been acknowledged in it either way).
func createSegment(fs fsys, dir string, id uint64, cp checkpoint3) (*segment, error) {
	cp.Segment = id
	data, err := appendCheckpoint3(segHeader(), cp)
	if err != nil {
		return nil, err
	}
	tmp, final := filepath.Join(dir, segTmpName(id)), filepath.Join(dir, segName(id))
	f, err := fs.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	abandon := func(err error) (*segment, error) {
		f.Close()
		fs.Remove(tmp) // best effort: Open removes leftover staging files anyway
		return nil, err
	}
	if n, err := f.WriteAt(data, 0); err != nil || n != len(data) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return abandon(err)
	}
	if err = f.Sync(); err != nil {
		return abandon(err)
	}
	if err = fs.Rename(tmp, final); err != nil {
		return abandon(err)
	}
	if err = fs.SyncDir(dir); err != nil {
		f.Close()
		return nil, fmt.Errorf("%w: %v", errSegUncertain, err)
	}
	return &segment{f: f, id: id, end: int64(len(data))}, nil
}

// append writes frames at the end and syncs. On any error the caller fences the log: a partial
// write may exist past s.end and the durability of the sync is unknown.
func (s *segment) append(frames []byte) error {
	n, err := s.f.WriteAt(frames, s.end)
	if err == nil && n != len(frames) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = s.f.SyncData()
	}
	if err != nil {
		return fmt.Errorf("segment write: %w", err)
	}
	s.end += int64(len(frames))
	return nil
}

// ----- reading frames -----

// frameReader reads frames sequentially from a file through a window, with no allocation per
// frame. A frame that straddles the end of the window makes it refill from that frame's start,
// large enough for any valid frame, so "short" is only ever reported when the end of the file is
// really inside the frame.
type frameReader struct {
	f     fsFile
	size  int64
	buf   []byte
	bufAt int64
}

func (r *frameReader) fill(off int64, want int) error {
	want = int(min(int64(want), r.size-off))
	if cap(r.buf) < want {
		r.buf = make([]byte, want)
	}
	r.buf = r.buf[:want]
	r.bufAt = off
	n, err := r.f.ReadAt(r.buf, off)
	if err != nil && !(errors.Is(err, io.EOF) && n == want) {
		r.buf = r.buf[:0]
		return err
	}
	return nil
}

func (r *frameReader) next(off int64) (typ byte, body []byte, n int, err error) {
	if off < r.bufAt || off >= r.bufAt+int64(len(r.buf)) {
		if err = r.fill(off, replayChunk); err != nil {
			return 0, nil, 0, err
		}
	}
	typ, body, n, err = readFrame3(r.buf[off-r.bufAt:])
	if errors.Is(err, errFrame3Short) && r.bufAt+int64(len(r.buf)) < r.size {
		// the window ended before the file did: read enough for the largest valid frame
		if err = r.fill(off, max(replayChunk, frame3Header+frame3Max)); err != nil {
			return 0, nil, 0, err
		}
		typ, body, n, err = readFrame3(r.buf[off-r.bufAt:])
	}
	return
}

// CorruptLogError says where a committed segment failed validation. It matches ErrCorruptLog with
// errors.Is and carries the segment, the byte offset (-1 when the whole segment is meant, as for a
// missing one) and the reason, for errors.As.
type CorruptLogError struct {
	Segment uint64
	Offset  int64
	Reason  string
}

func (e *CorruptLogError) Error() string {
	if e.Offset < 0 {
		return fmt.Sprintf("%v: segment %d: %s", ErrCorruptLog, e.Segment, e.Reason)
	}
	return fmt.Sprintf("%v: segment %d at offset %d: %s", ErrCorruptLog, e.Segment, e.Offset, e.Reason)
}

func (e *CorruptLogError) Unwrap() error { return ErrCorruptLog }

func corrupt(id uint64, at int64, format string, a ...any) error {
	return &CorruptLogError{Segment: id, Offset: at, Reason: fmt.Sprintf(format, a...)}
}

// scanSegment opens segment id, validates its header and start checkpoint, replays every frame
// after it into the state the checkpoint described, and returns the segment ready for appending
// together with that state.
//
// The torn-tail rule is the one the journal has always had, made precise by the frame header
// check: an incomplete final frame (the end of the file is inside a frame whose header
// verifies) is a torn append and the file is truncated at that frame's start; a retention
// checkpoint that is incomplete at the end is truncated at its first frame; anything else that
// does not validate (a header or checksum failure, a frame that does not follow the state, an
// unreadable start checkpoint) is ErrCorruptLog, and nothing is changed. onRecord, when set, is
// called for every record after the start checkpoint with the frame's position and size, in
// order; its slices alias a buffer that is reused.
func scanSegment(fs fsys, dir string, id uint64, onRecord func(rec record3, at int64, size int) error) (*segment, *logState, error) {
	f, err := fs.OpenFile(filepath.Join(dir, segName(id)), os.O_RDWR, 0)
	if err != nil {
		return nil, nil, err
	}
	seg, st, err := func() (*segment, *logState, error) {
		size, err := f.Size()
		if err != nil {
			return nil, nil, err
		}
		if size < segHeaderSize {
			return nil, nil, corrupt(id, 0, "%d bytes, shorter than the header", size)
		}
		hdr := make([]byte, segHeaderSize)
		if _, err = f.ReadAt(hdr, 0); err != nil {
			return nil, nil, err
		}
		if err = checkSegHeader(hdr); err != nil {
			return nil, nil, corrupt(id, 0, "%v", err)
		}
		st := newLogState()
		r := newReplayer3(st)
		r.WantSegment = &id
		var curAt int64
		var curSize int
		if onRecord != nil {
			r.OnRecord = func(rec record3) error { return onRecord(rec, curAt, curSize) }
		}
		fr := &frameReader{f: f, size: size}
		at, cpStart, torn := int64(segHeaderSize), int64(-1), false
		for at < size {
			typ, body, n, err := fr.next(at)
			if err != nil {
				if errors.Is(err, errFrame3Short) {
					torn = true
					break
				}
				return nil, nil, corrupt(id, at, "%v", err)
			}
			if at == segHeaderSize && typ != f3CpBegin {
				return nil, nil, corrupt(id, at, "the first frame is type %d, not the start checkpoint", typ)
			}
			if typ == f3CpBegin && !r.InCheckpoint() {
				cpStart = at
			}
			curAt, curSize = at, n
			if err = r.Frame(typ, body); err != nil {
				return nil, nil, corrupt(id, at, "%v", err)
			}
			at += int64(n)
		}
		if r.InCheckpoint() {
			if r.Checkpoints == 0 {
				return nil, nil, corrupt(id, cpStart, "the start checkpoint is incomplete")
			}
			at, torn = cpStart, true // an incomplete retention checkpoint: truncate at its first frame
		}
		if r.Checkpoints == 0 {
			return nil, nil, corrupt(id, segHeaderSize, "no start checkpoint")
		}
		if torn || at < size {
			if err = f.Truncate(at); err != nil {
				return nil, nil, err
			}
			if err = f.Sync(); err != nil {
				return nil, nil, err
			}
		}
		return &segment{f: f, id: id, end: at}, st, nil
	}()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return seg, st, nil
}

// ----- the directory of segments -----

type segLog struct {
	fs     fsys
	dir    string
	ids    []uint64 // sealed segment ids, ascending (the active segment is not in it)
	active *segment
	state  *logState // as of Open: the state the active segment replays to
	failed error     // set by a failed write or roll: the log refuses further writes until reopened
}

// openSegLog lists the directory, removes staging files, checks that the segment ids are
// contiguous, and scans the highest segment. A directory with no segment opens as an empty log
// the caller starts with start. Anything wrong with a committed segment is ErrCorruptLog.
func openSegLog(fs fsys, dir string) (*segLog, error) {
	if err := fs.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	names, err := fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, name := range names {
		if strings.HasPrefix(name, "seg-") && strings.HasSuffix(name, ".tmp") {
			// a staged segment that was never published: nothing acknowledged is in it
			if err = fs.Remove(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
			continue
		}
		if id, ok := parseSegName(name); ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			return nil, &CorruptLogError{Segment: ids[i-1] + 1, Offset: -1, Reason: fmt.Sprintf("the segment is missing (segments %d and %d are present)", ids[i-1], ids[i])}
		}
	}
	l := &segLog{fs: fs, dir: dir}
	if len(ids) == 0 {
		l.state = newLogState()
		return l, nil
	}
	top := ids[len(ids)-1]
	if l.active, l.state, err = scanSegment(fs, dir, top, nil); err != nil {
		return nil, err
	}
	l.ids = ids[:len(ids)-1]
	return l, nil
}

// start publishes the first segment of an empty log.
func (l *segLog) start(cp checkpoint3) error {
	if l.active != nil || len(l.ids) != 0 {
		return errors.New("segment log already started")
	}
	seg, err := createSegment(l.fs, l.dir, 1, cp)
	if err != nil {
		l.failed = err
		return err
	}
	l.active = seg
	st := newLogState()
	if err = st.load(cp); err != nil {
		l.failed = err
		return err
	}
	l.state = st
	return nil
}

func (l *segLog) append(frames []byte) error {
	if l.failed != nil {
		return l.failed
	}
	if l.active == nil {
		return errors.New("segment log not started")
	}
	if err := l.active.append(frames); err != nil {
		l.failed = err
		return err
	}
	return nil
}

// roll publishes the next segment, whose start checkpoint is cp, and makes it the active one. Any
// failure fences the log (the staging file is removed if the rename did not happen).
func (l *segLog) roll(cp checkpoint3) error {
	if l.failed != nil {
		return l.failed
	}
	if l.active == nil {
		return errors.New("segment log not started")
	}
	seg, err := createSegment(l.fs, l.dir, l.active.id+1, cp)
	if err != nil {
		l.failed = err
		return err
	}
	l.ids = append(l.ids, l.active.id)
	l.active.f.Close()
	l.active = seg
	return nil
}

func (l *segLog) close() error {
	if l.active == nil {
		return nil
	}
	return l.active.f.Close()
}
