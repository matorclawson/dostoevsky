// Command dost-conform drives one engine for the conformance suite: it reads one JSON request per line on standard input
// and writes one JSON reply per line on standard output. Another engine joins the suite by speaking the same lines.
// It is a test tool: it opens whatever directory a request names. The protocol is described in README.md.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"

	d "github.com/rodriveracom/dostoevsky"
)

type partition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

func (p partition) engine() d.Partition { return d.Partition{Topic: p.Topic, Number: p.Partition} }

type message struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
	ID    string `json:"id"`
}

func (m message) engine() d.Message { return d.Message{Key: m.Key, Value: m.Value, ID: m.ID} }

type floor struct {
	partition
	Floor int64 `json:"floor"`
}

type request struct {
	Op           string         `json:"op"`
	Dir          string         `json:"dir"`
	Segmented    bool           `json:"segmented"`
	Node         string         `json:"node"`
	Nodes        []string       `json:"nodes"`
	Topics       map[string]int `json:"topics"`
	Items        []message      `json:"items"`
	Group        string         `json:"group"`
	Offset       int64          `json:"offset"`
	Next         int64          `json:"next"`
	State        []byte         `json:"state"`
	Records      int            `json:"records"`
	Bytes        int            `json:"bytes"`
	Floors       []floor        `json:"floors"`
	Dest         string         `json:"dest"`
	DiscardLater bool           `json:"discard_later"`
	partition
	Key   string `json:"key"`
	Value []byte `json:"value"`
	ID    string `json:"id"`
}

// record is one stored record. Time is nanoseconds since the Unix epoch, as a string so that no reader rounds it.
type record struct {
	partition
	Offset int64  `json:"offset"`
	Key    string `json:"key"`
	Value  []byte `json:"value"`
	ID     string `json:"id"`
	Time   int64  `json:"time,string"`
}

func encode(r d.Record) record {
	return record{partition{r.Topic, r.Number}, r.Offset, r.Key, r.Value, r.ID, r.Time.UnixNano()}
}

type reply struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
	Result  any    `json:"result,omitempty"`
}

// codes names the errors an engine must distinguish. The order matters: an error that wraps another comes first.
var codes = []struct {
	name string
	err  error
}{
	{"journal_full", d.ErrJournalFull},
	{"limit", d.ErrLimit},
	{"invalid", d.ErrInvalid},
	{"owner", d.ErrOwner},
	{"closed", d.ErrClosed},
	{"conflict", d.ErrConflict},
	{"expired", d.ErrExpired},
	{"empty", d.ErrEmpty},
	{"corrupt_log", d.ErrCorruptLog},
	{"incomplete_backup", d.ErrIncompleteBackup},
	{"backup_not_durable", d.ErrBackupNotDurable},
	{"upgrade_required", d.ErrUpgradeRequired},
	{"segmented_journal", d.ErrSegmentedJournal},
	{"later_frames", d.ErrLaterFrames},
	{"not_repairable", d.ErrNotRepairable},
	{"exists", fs.ErrExist},
}

func failure(err error) reply {
	code := "other"
	for _, c := range codes {
		if errors.Is(err, c.err) {
			code = c.name
			break
		}
	}
	return reply{Error: code, Message: err.Error()}
}

var errNotOpen = errors.New("dost-conform: no node is open")

type driver struct{ node *d.Node }

func report(r d.JournalReport) map[string]any {
	return map[string]any{"size": r.Size, "good_bytes": r.GoodBytes, "frames": r.Frames, "damage": r.Damage,
		"later_frames": r.LaterFrames, "quarantined": r.Quarantine != ""}
}

// run performs one request. A result can come with an error: a refused repair still reports what it found.
func (s *driver) run(q request) (any, error) {
	ctx := context.Background()
	switch q.Op {
	case "open":
		if s.node != nil {
			return nil, errors.New("dost-conform: a node is already open")
		}
		open := d.Open
		if q.Segmented {
			open = d.OpenSegmented
		}
		n, err := open(q.Dir, d.Config{Node: q.Node, Nodes: q.Nodes, Topics: q.Topics})
		if err != nil {
			return nil, err
		}
		s.node = n
		t := n.Truncated()
		return map[string]any{"truncated_at": t.At, "truncated_bytes": t.Bytes}, nil
	case "inspect":
		r, err := d.InspectJournal(q.Dir)
		return report(r), err
	case "repair":
		r, err := d.RepairJournal(q.Dir, d.RepairOptions{DiscardLaterFrames: q.DiscardLater})
		return report(r), err
	case "upgrade":
		saved, err := d.UpgradeJournal(q.Dir)
		return map[string]any{"saved_copy": saved != ""}, err
	}
	n := s.node
	if n == nil {
		return nil, errNotOpen
	}
	switch q.Op {
	case "close":
		s.node = nil
		return nil, n.Close()
	case "publish":
		r, err := n.Publish(ctx, q.Topic, d.Message{Key: q.Key, Value: q.Value, ID: q.ID})
		if err != nil {
			return nil, err
		}
		return encode(r), nil
	case "publish_batch":
		items := make([]d.Publication, len(q.Items))
		for i, m := range q.Items {
			items[i] = d.Publication{Topic: m.Topic, Message: m.engine()}
		}
		rs, err := n.PublishBatch(ctx, items)
		out := make([]record, len(rs))
		for i, r := range rs {
			out[i] = encode(r)
		}
		return map[string]any{"records": out}, err
	case "read":
		r, err := n.Read(ctx, q.partition.engine(), q.Offset, false)
		if err != nil {
			return nil, err
		}
		return encode(r), nil
	case "read_batch":
		page, err := n.ReadBatch(ctx, q.partition.engine(), q.Offset, d.Limits{Records: q.Records, Bytes: q.Bytes}, false)
		if err != nil {
			return nil, err
		}
		out := make([]record, len(page.Records))
		for i, r := range page.Records {
			out[i] = encode(r)
		}
		return map[string]any{"records": out, "next": page.Next}, nil
	case "position":
		pos, err := n.Position(ctx, q.partition.engine(), q.Group)
		if err != nil {
			return nil, err
		}
		return map[string]any{"next": pos.Next, "state": pos.State}, nil
	case "commit_batch":
		return nil, n.CommitBatch(ctx, q.partition.engine(), q.Group, q.Offset, q.Next, q.State)
	case "retain":
		floors := make(map[d.Partition]int64, len(q.Floors))
		for _, f := range q.Floors {
			floors[f.partition.engine()] = f.Floor
		}
		return nil, n.Retain(ctx, floors)
	case "backup":
		return nil, n.Backup(ctx, q.Dest)
	case "migrate":
		return nil, n.MigrateSegmented(ctx, q.Dest)
	case "verify":
		return nil, n.VerifySegments(ctx)
	}
	return nil, errors.New("dost-conform: unknown op " + q.Op)
}

func serve(in io.Reader, out io.Writer) error {
	requests := json.NewDecoder(bufio.NewReader(in))
	requests.DisallowUnknownFields()
	replies := json.NewEncoder(out)
	s := &driver{}
	for {
		var q request
		if err := requests.Decode(&q); err == io.EOF {
			if s.node != nil {
				return s.node.Close()
			}
			return nil
		} else if err != nil {
			return err
		}
		result, err := s.run(q)
		answer := reply{OK: true, Result: result}
		if err != nil {
			answer = failure(err)
			answer.Result = result
		}
		if err := replies.Encode(answer); err != nil {
			return err
		}
	}
}

func main() {
	if err := serve(os.Stdin, os.Stdout); err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}
