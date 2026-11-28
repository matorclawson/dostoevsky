package dostoevsky

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Open never guesses about damage: a frame that does not verify stops recovery. InspectJournal and RepairJournal are
// the explicit way on from there, for an operator, with the application stopped.

// What InspectJournal found at the first frame that does not verify.
const (
	DamageNone     = ""          // every frame verifies
	DamageTornTail = "torn-tail" // the last frame is incomplete; Open cuts this off by itself
	DamageHeader   = "header"    // a header does not verify (a zero-filled tail looks like this)
	DamageLength   = "length"    // a verified header announces an impossible length
	DamageChecksum = "checksum"  // a complete frame whose payload does not match its checksum
	DamageEntry    = "entry"     // the checksum matches but the payload is not an entry: not a repair for truncation
)

var (
	// ErrSegmentedJournal indicates the legacy inspection or repair tool was given a segmented store.
	ErrSegmentedJournal = errors.New("use VerifySegments to inspect a segmented store")
	// ErrLaterFrames is returned by RepairJournal when frames that verify follow the damage: truncating would discard
	// writes that were very likely acknowledged.
	ErrLaterFrames = errors.New("frames that verify follow the damage")
	// ErrNotRepairable is returned by RepairJournal for damage that truncation is not the answer to.
	ErrNotRepairable = errors.New("this damage is not repaired by truncating")
)

// JournalReport describes a journal at the level of its frames. It does not replay them.
type JournalReport struct {
	Size        int64  // bytes in the journal
	GoodBytes   int64  // bytes before the first frame that does not verify
	Frames      int    // frames in those bytes
	Damage      string // one of the Damage constants
	LaterFrames int    // frames that verify, found after the damage
	Quarantine  string // set by RepairJournal: the file holding the bytes it removed
}

// RepairOptions configures RepairJournal.
type RepairOptions struct {
	// DiscardLaterFrames allows truncating although frames that verify follow the damage.
	DiscardLaterFrames bool
}

// openForRepair takes the same locks Open takes, so it fails while a node has the directory open. It creates nothing
// in a directory that holds no journal.
func openForRepair(dir string) (*journal, error) {
	if _, err := os.Stat(filepath.Join(dir, "journal.v1")); err != nil {
		return nil, fmt.Errorf("no journal to inspect: %w", err)
	}
	j, err := openJournal(dir)
	if err != nil {
		return nil, err
	}
	var marker [len(segmentMarker)]byte
	if _, e := j.file.ReadAt(marker[:], 0); e != nil && !errors.Is(e, io.EOF) {
		j.close()
		return nil, e
	}
	if string(marker[:]) == segmentMarker || strings.HasPrefix(string(marker[:]), workflowMagic) {
		j.close()
		return nil, ErrSegmentedJournal
	}
	return j, nil
}

// frameAt checks the frame starting at off. It returns the damage found there, or the frame's total length.
func frameAt(f *os.File, off, size int64) (int64, string, error) {
	var h [20]byte
	if size-off < int64(len(h)) {
		return 0, DamageTornTail, nil
	}
	if _, err := f.ReadAt(h[:], off); err != nil {
		return 0, "", err
	}
	if !validHeader(h[:]) {
		return 0, DamageHeader, nil
	}
	n := int(binary.BigEndian.Uint32(h[8:]))
	if n > maxFrame || n <= 0 {
		return 0, DamageLength, nil
	}
	total := int64(len(h) + n)
	if off+total > size {
		return 0, DamageTornTail, nil
	}
	payload := make([]byte, n)
	if _, err := f.ReadAt(payload, off+int64(len(h))); err != nil {
		return 0, "", err
	}
	if crc32.Checksum(payload, crcTable) != binary.BigEndian.Uint32(h[12:]) {
		return 0, DamageChecksum, nil
	}
	var e entry
	if json.Unmarshal(payload, &e) != nil {
		return 0, DamageEntry, nil
	}
	return total, DamageNone, nil
}

// laterFrames counts the frames that verify anywhere from off on, by looking for their signature.
func laterFrames(f *os.File, off, size int64) (int, error) {
	mark := signature[:7] // both versions begin the same
	window := make([]byte, 64<<10)
	count := 0
	for off+int64(len(mark)) <= size {
		n, err := f.ReadAt(window, off)
		if err != nil && err != io.EOF {
			return 0, err
		}
		i := bytes.Index(window[:n], mark)
		if i < 0 {
			if n < len(window) {
				break
			}
			off += int64(n - len(mark) + 1) // the mark may straddle two windows
			continue
		}
		off += int64(i)
		total, damage, err := frameAt(f, off, size)
		if err != nil {
			return 0, err
		}
		if damage == DamageNone {
			count++
			off += total
		} else {
			off++
		}
	}
	return count, nil
}

func inspect(f *os.File) (JournalReport, error) {
	info, err := f.Stat()
	if err != nil {
		return JournalReport{}, err
	}
	r := JournalReport{Size: info.Size()}
	for r.GoodBytes < r.Size {
		total, damage, err := frameAt(f, r.GoodBytes, r.Size)
		if err != nil {
			return r, err
		}
		if damage != DamageNone {
			r.Damage = damage
			break
		}
		r.GoodBytes += total
		r.Frames++
	}
	if r.Damage != DamageNone { // also behind a torn tail: a wrong length looks like one
		r.LaterFrames, err = laterFrames(f, r.GoodBytes+1, r.Size)
	}
	return r, err
}

// InspectJournal reports on the journal in dir without changing the journal. The directory must not be open. Like Open it
// takes the directory's lock, so it needs write permission there: it creates journal.lock if missing and syncs the
// directory.
func InspectJournal(dir string) (JournalReport, error) {
	j, err := openForRepair(dir)
	if err != nil {
		return JournalReport{}, err
	}
	defer j.close()
	return inspect(j.file)
}

// RepairJournal truncates the journal in dir after its last frame that verifies, having first copied everything it
// removes to a new file beside the journal and synced it. Records and checkpoints in the removed bytes are gone from
// the journal, including, possibly, acknowledged ones: the damaged frame itself may have been acknowledged. It does
// nothing to a sound journal, and refuses with ErrLaterFrames or ErrNotRepairable as those describe.
func RepairJournal(dir string, o RepairOptions) (JournalReport, error) {
	j, err := openForRepair(dir)
	if err != nil {
		return JournalReport{}, err
	}
	defer j.close()
	r, err := inspect(j.file)
	if err != nil || r.Damage == DamageNone {
		return r, err
	}
	if r.Damage == DamageEntry {
		return r, ErrNotRepairable
	}
	if r.LaterFrames > 0 && !o.DiscardLaterFrames {
		return r, ErrLaterFrames
	}
	name := filepath.Join(dir, fmt.Sprintf("journal.v1.damaged-%d", time.Now().UnixNano()))
	if err := copyDurably(dir, name, io.NewSectionReader(j.file, r.GoodBytes, r.Size-r.GoodBytes)); err != nil {
		return r, fmt.Errorf("journal left unchanged, the copy of the removed bytes failed: %w", err)
	}
	r.Quarantine = name
	err = j.file.Truncate(r.GoodBytes)
	if err == nil {
		err = syncFile(j.file)
	}
	if err != nil {
		// The copy is complete and synced; whether the cut is durable is not known.
		return r, fmt.Errorf("truncating at %d after copying the removed bytes to %s: %w", r.GoodBytes, name, err)
	}
	return r, nil
}

// copyDurably writes src to a new file in dir and syncs the file and the directory.
func copyDurably(dir, name string, src io.Reader) error {
	out, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, src)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
