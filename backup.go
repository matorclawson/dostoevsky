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
// remains locked during the copy. The destination is created with Mkdir, which
// fails if it exists, and the journal is written to a temporary name inside it,
// synced, renamed to journal.v1, and the directory and then its parent are
// synced. A crash mid-copy leaves a directory without journal.v1, which is not
// a valid backup; a failed backup removes the directory it created. If only the
// final parent sync fails the backup is complete and the error wraps
// ErrBackupNotDurable.
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
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			os.RemoveAll(dir)
		}
	}()
	tmp := filepath.Join(dir, ".journal.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
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
	if err = os.Rename(tmp, filepath.Join(dir, "journal.v1")); err != nil {
		return err
	}
	if err = syncDir(dir); err != nil {
		return err
	}
	published = true
	if err = syncDir(filepath.Dir(dir)); err != nil {
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
