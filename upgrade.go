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

// legacyOnly reports whether the journal holds complete frames and none signed DOST0002. It reads headers only, stops
// at the first DOST0002 frame (a journal born under a current version starts with one), and says no for a header it
// cannot read, leaving that to recovery to report.
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
	return frames > 0, nil
}

func upgradeRequired(dir string) error {
	return fmt.Errorf("%w: %s was written by 0.2.x and upgrading is one way (0.2.x cannot open it afterwards). "+
		"Stop the application, back up the directory by copying it, then run: dost-repair -data %s -upgrade", ErrUpgradeRequired, dir, dir)
}

// UpgradeJournal marks a journal written by 0.2.x as upgraded, after which Open accepts it and 0.2.x refuses it. It
// first copies the journal to a new file beside it and returns that file's name; keep your own backup as well. A
// journal that needs no upgrade is left alone and "" is returned. The directory must not be open.
func UpgradeJournal(dir string) (string, error) {
	j, err := openForRepair(dir)
	if err != nil {
		return "", err
	}
	defer j.close()
	old, err := legacyOnly(j.file)
	if err != nil || !old {
		return "", err
	}
	r, err := inspect(j.file)
	if err != nil {
		return "", err
	}
	if r.Damage != DamageNone && (r.Damage != DamageTornTail || r.LaterFrames > 0) {
		return "", fmt.Errorf("the journal is damaged (%s at %d): repair it before upgrading", r.Damage, r.GoodBytes)
	}
	name := filepath.Join(dir, fmt.Sprintf("journal.v1.pre-upgrade-%d", time.Now().UnixNano()))
	if err := copyDurably(dir, name, io.NewSectionReader(j.file, 0, r.Size)); err != nil {
		return "", fmt.Errorf("journal left unchanged, the copy before upgrading failed: %w", err)
	}
	if err := j.recover(func(entry, location) error { return nil }); err != nil {
		return name, err
	}
	_, err = j.append(entry{Fence: 1})
	return name, err
}
