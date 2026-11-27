package dostoevsky

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// A journal written only by 0.2.x (frames signed DOST0001) is not opened as it is: this version would have to mark it,
// after which 0.2.x refuses it. That step is one way, so it is taken only when asked.

// ErrUpgradeRequired is returned by Open for a journal that only 0.2.x has written. The journal is not modified.
var ErrUpgradeRequired = errors.New("journal upgrade required")

// legacyOnly reports whether the journal must not be written to without an upgrade: it holds complete frames and none
// signed DOST0002, or it is not empty, holds no complete frame, and does not begin as a current version writes. Opening
// the latter would cut the bytes and write DOST0002 frames into what may be a 0.2.x directory. It reads headers only,
// stops at the first DOST0002 frame (a journal born under a current version starts with one), and says no for a header
// it cannot read, leaving that to recovery to report (which changes nothing then).
func legacyOnly(f *os.File) (bool, error) {
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	size := info.Size()
	r := bufio.NewReaderSize(io.NewSectionReader(f, 0, size), 32<<10)
	var at int64
	frames := 0
	for at+20 <= size {
		var h [20]byte
		if _, err := io.ReadFull(r, h[:]); err != nil {
			return false, err
		}
		if !validHeader(h[:]) {
			return false, nil
		}
		n := int(binary.BigEndian.Uint32(h[8:]))
		if n <= 0 || n > maxFrame {
			return false, nil
		}
		if at+20+int64(n) > size { // an incomplete frame counts for nothing, whatever its signature
			break
		}
		if string(h[:8]) == "DOST0002" {
			return false, nil
		}
		if _, err := r.Discard(n); err != nil {
			return false, err
		}
		at += 20 + int64(n)
		frames++
	}
	if frames > 0 || size == 0 {
		return frames > 0, nil
	}
	// Only an interrupted first write. A current version's begins with its whole signature; anything less does not say
	// which version wrote it.
	var first [8]byte
	if size < int64(len(first)) {
		return true, nil
	}
	if _, err := f.ReadAt(first[:], 0); err != nil {
		return false, err
	}
	return string(first[:]) != "DOST0002", nil
}

func upgradeRequired(dir string) error {
	return fmt.Errorf("%w: %s was written by 0.2.x (or holds only an interrupted first write, which does not say), and upgrading is one way (0.2.x cannot open it afterwards). "+
		"Stop the application, back up the directory by copying it, then run: dost-repair -data %s -upgrade", ErrUpgradeRequired, dir, dir)
}

// UpgradeJournal marks a journal written by 0.2.x as upgraded, after which Open accepts it and 0.2.x refuses it. It
// first copies the journal to a new file beside it and returns that file's name, also when a later step fails; keep your
// own backup as well. The copy is as large as the journal, so the directory needs that much free space; it may be
// deleted once the upgraded directory has been opened and checked, and you no longer intend to go back. A journal that
// needs no upgrade is left alone and "" is returned. A damaged journal is refused: repair it first. The directory must
// not be open.
func UpgradeJournal(dir string) (string, error) {
	j, err := openForRepair(dir)
	if err != nil {
		return "", err
	}
	defer j.close()
	// Damage first: a header that cannot be read must not pass for "no upgrade needed".
	r, err := inspect(j.file)
	if err != nil {
		return "", err
	}
	if r.Damage != DamageNone && (r.Damage != DamageTornTail || r.LaterFrames > 0) {
		return "", fmt.Errorf("the journal is damaged (%s at %d): repair it before upgrading", r.Damage, r.GoodBytes)
	}
	old, err := legacyOnly(j.file)
	if err != nil || !old {
		return "", err
	}
	name := filepath.Join(dir, fmt.Sprintf("journal.v1.pre-upgrade-%d", time.Now().UnixNano()))
	if err := copyDurably(dir, name, io.NewSectionReader(j.file, 0, r.Size)); err != nil {
		return "", fmt.Errorf("journal left unchanged, the copy before upgrading failed: %w", err)
	}
	if err := j.recover(func(entry, location) error { return nil }); err != nil {
		return name, fmt.Errorf("upgrade stopped while reading the journal, after the copy: %w", err)
	}
	if j.end == 0 { // there was only an interrupted first write: the journal is empty now and opens as new
		return name, nil
	}
	if _, err := j.append(entry{Fence: 1}); err != nil {
		return name, fmt.Errorf("upgrade stopped while writing the marker, after the copy: %w", err)
	}
	return name, nil
}
