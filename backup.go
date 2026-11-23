package dostoevsky

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ErrBackupNotDurable reports that a backup is complete and readable but its
// directory entry may not survive a crash. The backup can be verified and used;
// repeat it or sync the parent directory before relying on it.
var ErrBackupNotDurable = errors.New("backup complete but its directory entry may not be durable")

// Backup writes a consistent journal snapshot into a new directory. The node
// remains locked during the copy. The whole backup is built in a hidden sibling
// of dir (.<name>.backup-<random>): journal.v1 is written and synced, the
// sibling is synced, dir is checked not to exist, the sibling is renamed to dir
// and the parent is synced. A crash leaves at most the hidden sibling, never a
// visible partial or empty dir; a failed backup removes its sibling. If only
// the final parent sync fails the backup is complete and the error wraps
// ErrBackupNotDurable.
//
// Caller contract: dir must be a new path that stays under the caller's control
// until Backup returns. No other process, and no other goroutine of this process
// (including a concurrent Backup to the same path), may create or use that path
// meanwhile. The existence check and the rename are two steps, and the standard
// library has no portable no-replace directory rename, so something that creates an
// empty directory at exactly that path in between would be replaced.
func (n *Node) Backup(ctx context.Context, dir string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	if n.j.failed != nil {
		return n.j.failed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dir = filepath.Clean(dir)
	parent := filepath.Dir(dir)
	if _, err := os.Lstat(dir); err == nil {
		return fmt.Errorf("backup destination exists: %w", os.ErrExist)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	stage, err := os.MkdirTemp(parent, "."+filepath.Base(dir)+".backup-")
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if !renamed {
			os.RemoveAll(stage)
		}
	}()
	f, err := os.OpenFile(filepath.Join(stage, "journal.v1"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	buf := make([]byte, 128*1024)
	var offset int64
	for offset < n.j.end {
		if err = ctx.Err(); err != nil {
			break
		}
		size := int64(len(buf))
		if remain := n.j.end - offset; remain < size {
			size = remain
		}
		var count int
		count, err = n.j.file.ReadAt(buf[:size], offset)
		if err != nil && !errors.Is(err, io.EOF) {
			break
		}
		if int64(count) != size {
			err = io.ErrUnexpectedEOF
			break
		}
		var written int
		written, err = f.Write(buf[:count])
		if err != nil {
			break
		}
		if written != count {
			err = io.ErrShortWrite
			break
		}
		offset += int64(count)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = syncDir(stage); err != nil {
		return err
	}
	if _, err = os.Lstat(dir); err == nil {
		return fmt.Errorf("backup destination appeared during the copy: %w", os.ErrExist)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = os.Rename(stage, dir); err != nil {
		return err
	}
	renamed = true
	if err = syncDir(parent); err != nil {
		return fmt.Errorf("%w: %v", ErrBackupNotDurable, err)
	}
	return nil
}

// syncDir is a hook so tests can see which directories are synced.
var syncDir = func(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	err = d.Sync()
	return errors.Join(err, d.Close())
}
