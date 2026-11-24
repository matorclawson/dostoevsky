// Command sqlite runs the publisher workload against SQLite used as a queue:
// an INSERT per record in WAL mode with synchronous=FULL, in process through
// the pure-Go modernc.org/sqlite driver.
package main

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	_ "modernc.org/sqlite"
)

type store struct{ db *sql.DB }

func (s store) Publish(data []byte) (uint64, error) {
	res, err := s.db.Exec("INSERT INTO events(data) VALUES (?)", data)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	return uint64(id), err
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

func main() {
	run.Main("sqlite", func(dir string, fresh bool) (run.Store, error) {
		dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(30000)", filepath.Join(dir, "events.db"))
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(64)
		if fresh {
			if _, err = db.Exec("CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB NOT NULL)"); err != nil {
				db.Close()
				return nil, err
			}
		}
		// Reopen cost includes the first statement that reads the file.
		var n int
		if err = db.QueryRow("SELECT count(*) FROM events").Scan(&n); err != nil {
			db.Close()
			return nil, err
		}
		return store{db}, nil
	})
}
