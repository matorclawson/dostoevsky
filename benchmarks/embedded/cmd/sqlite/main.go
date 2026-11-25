// Command sqlite runs the publisher workload against SQLite used as a queue:
// an INSERT per record in WAL mode with synchronous=FULL, in process through
// the pure-Go modernc.org/sqlite driver.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	_ "modernc.org/sqlite"
)

type store struct{ db *sql.DB }

// Publish is one transaction with one FULL sync. There is a single writer
// connection, so concurrent producers queue in Go ahead of it instead of
// fighting over the WAL write lock.
func (s store) Publish(data []byte) (uint64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	res, err := tx.Exec("INSERT INTO events(data) VALUES (?)", data)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	return uint64(id), tx.Commit()
}

func (s store) Read(id uint64) ([]byte, error) {
	var data []byte
	err := s.db.QueryRow("SELECT data FROM events WHERE id = ?", int64(id)).Scan(&data)
	return data, err
}

func (s store) Close() {
	if err := s.db.Close(); err != nil {
		panic(err)
	}
}

func main() { run.Main("sqlite", openStore) }

func openStore(dir string, fresh bool) (run.Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(30000)", filepath.Join(dir, "events.db"))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // one writer connection: SQLite as a queue, labeled single writer
	if fresh {
		if _, err = db.Exec("CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB NOT NULL)"); err != nil {
			db.Close()
			return nil, err
		}
	}
	// Readiness probe for the timed reopen: one index seek, not a table scan.
	var last int64
	if err = db.QueryRow("SELECT id FROM events ORDER BY id DESC LIMIT 1").Scan(&last); err != nil && !errors.Is(err, sql.ErrNoRows) {
		db.Close()
		return nil, err
	}
	return store{db}, nil
}
