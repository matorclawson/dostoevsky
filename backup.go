package dostoevsky

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Backup writes a consistent journal snapshot into a new directory. The node
// remains locked during the copy. The copy is built in a hidden sibling directory,
// synced, renamed to dir and the parent is synced, so dir either does not exist
// or holds a complete backup. A failed backup leaves no destination.
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
	if _, err := os.Lstat(dir); err == nil {
		return fmt.Errorf("backup destination exists: %w", os.ErrExist)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Copy into a hidden sibling so the destination name never shows a partial backup.
	parent := filepath.Dir(dir)
	stage, err := os.MkdirTemp(parent, ".backup-*")
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
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
	if err = os.Rename(stage, dir); err != nil {
		return err
	}
	committed = true
	// The new directory entry lives in the parent.
	return syncDir(parent)
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
