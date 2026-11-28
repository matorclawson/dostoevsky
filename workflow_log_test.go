package dostoevsky

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkflowBoundaryRoll(t *testing.T) {
	for _, rolled := range []bool{false, true} {
		for _, uncertain := range []bool{false, true} {
			t.Run(fmt.Sprintf("roll=%t/witness_rename=%t", rolled, uncertain), func(t *testing.T) {
				dir := t.TempDir()
				log, e := OpenWorkflowLog(dir)
				if e != nil {
					t.Fatal(e)
				}
				publish := func(l *WorkflowLog, v string) error {
					_, e := l.PublishBatch(context.Background(), []Publication{{Topic: "transitions", Message: Message{Value: []byte(v)}}})
					return e
				}
				if e = publish(log, "acknowledged"); e != nil {
					t.Fatal(e)
				}
				if rolled {
					log.n.rollBytes = 1
				}
				failure := errors.New("uncertain witness write")
				log.persist = func(dir string, b []byte) error {
					if uncertain {
						if e := publishWitness(dir, b); e != nil {
							return e
						}
					}
					return failure
				}
				if e = publish(log, "maybe"); e != failure {
					t.Fatal(e)
				}
				if e = publish(log, "fenced"); e != failure {
					t.Fatal(e)
				}
				if e = log.Close(); e != nil {
					t.Fatal(e)
				}
				if !uncertain {
					logs, e := filepath.Glob(filepath.Join(dir, "segments", "seg-*.log"))
					if e != nil {
						t.Fatal(e)
					}
					last := logs[len(logs)-1]
					f, e := os.OpenFile(last, os.O_RDWR, 0)
					if e != nil {
						t.Fatal(e)
					}
					st, e := f.Stat()
					if e != nil {
						t.Fatal(e)
					}
					if _, e = f.WriteAt([]byte{0xfe}, st.Size()-1); e != nil {
						t.Fatal(e)
					}
					if e = f.Close(); e != nil {
						t.Fatal(e)
					}
				}
				for i := 0; i < 2; i++ {
					log, e = OpenWorkflowLog(dir)
					if e != nil {
						t.Fatal(e)
					}
					page, e := log.ReadBatch(context.Background(), Partition{Topic: "transitions"}, 0, Limits{10, 1000}, false)
					if e != nil {
						t.Fatal(e)
					}
					want := 1 + i
					if uncertain {
						want++
					}
					if len(page.Records) != want || string(page.Records[0].Value) != "acknowledged" {
						t.Fatal(page)
					}
					if uncertain && string(page.Records[1].Value) != "maybe" {
						t.Fatal(page)
					}
					if i == 0 {
						if e = publish(log, "after"); e != nil {
							t.Fatal(e)
						}
					}
					if e = log.Close(); e != nil {
						t.Fatal(e)
					}
				}
			})
		}
	}
}

func TestWorkflowBoundaryInvalidEvidence(t *testing.T) {
	for _, target := range []string{"witness", "sealed"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			l, e := OpenWorkflowLog(dir)
			if e != nil {
				t.Fatal(e)
			}
			l.n.rollBytes = 1
			for i := 0; i < 3; i++ {
				if _, e = l.PublishBatch(context.Background(), []Publication{{Topic: "transitions", Message: Message{Value: []byte("abc")}}}); e != nil {
					t.Fatal(e)
				}
			}
			if e = l.Close(); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(dir, "journal.v1")
			if target == "sealed" {
				path = filepath.Join(dir, "segments", segName(1))
			}
			b, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			b[len(b)-1] ^= 1
			if e = os.WriteFile(path, b, 0600); e != nil {
				t.Fatal(e)
			}
			if l, e = OpenWorkflowLog(dir); e == nil {
				l.Close()
				t.Fatal("damaged evidence accepted")
			}
			after, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(b, after) {
				t.Fatal("evidence modified", e)
			}
		})
	}
}
func TestWorkflowBoundaryAdoptsLegacy(t *testing.T) {
	for _, damaged := range []bool{false, true} {
		t.Run(fmt.Sprint(damaged), func(t *testing.T) {
			cfg := Config{Node: "workflow", Nodes: []string{"workflow"}, Topics: map[string]int{"transitions": 1}}
			dir := t.TempDir()
			n, e := OpenSegmented(dir, cfg)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = n.Publish(context.Background(), "transitions", Message{Value: []byte("old")}); e != nil {
				t.Fatal(e)
			}
			n.Close()
			if damaged {
				p := filepath.Join(dir, "segments", segName(1))
				info, e := os.Stat(p)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.Truncate(p, info.Size()-1); e != nil {
					t.Fatal(e)
				}
			}
			l, e := OpenWorkflowLog(dir)
			if damaged {
				if e == nil {
					l.Close()
					t.Fatal("damaged legacy adopted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			l.Close()
			if n, e = OpenSegmented(dir, cfg); e == nil {
				n.Close()
				t.Fatal("ordinary writer bypassed witness")
			}
			l, e = OpenWorkflowLog(dir)
			if e != nil {
				t.Fatal(e)
			}
			defer l.Close()
			p, e := l.ReadBatch(context.Background(), Partition{Topic: "transitions"}, 0, Limits{1, 8}, false)
			if e != nil || string(p.Records[0].Value) != "old" {
				t.Fatal(p, e)
			}
		})
	}
}

func TestForeignWorkflowWitnessCannotDiscardData(t *testing.T) {
	donor := t.TempDir()
	a, e := OpenWorkflowLog(donor)
	if e != nil {
		t.Fatal(e)
	}
	a.Close()
	dir := t.TempDir()
	b, e := OpenWorkflowLog(dir)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.PublishBatch(context.Background(), []Publication{{Topic: "transitions", Message: Message{Value: []byte("preserve")}}}); e != nil {
		t.Fatal(e)
	}
	b.Close()
	path := filepath.Join(dir, "segments", segName(1))
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	marker, e := os.ReadFile(filepath.Join(donor, "journal.v1"))
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "journal.v1"), marker, 0600); e != nil {
		t.Fatal(e)
	}
	if b, e = OpenWorkflowLog(dir); e == nil {
		b.Close()
		t.Fatal("foreign witness accepted")
	}
	after, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("changed committed bytes", e)
	}
}

func TestMissingUncommittedSuccessor(t *testing.T) {
	dir := t.TempDir()
	l, e := OpenWorkflowLog(dir)
	if e != nil {
		t.Fatal(e)
	}
	put := func(s string) error {
		_, e := l.PublishBatch(context.Background(), []Publication{{Topic: "transitions", Message: Message{Value: []byte(s)}}})
		return e
	}
	if e = put("keep"); e != nil {
		t.Fatal(e)
	}
	l.n.rollBytes = 1
	l.persist = func(string, []byte) error { return errors.New("witness unavailable") }
	if e = put("discard"); e == nil {
		t.Fatal("unexpected commit")
	}
	if _, e = l.ReadBatch(context.Background(), Partition{Topic: "transitions"}, 0, Limits{1, 16}, false); e == nil {
		t.Fatal("fenced read succeeded")
	}
	l.Close()
	if e = os.Remove(filepath.Join(dir, "segments", segName(2))); e != nil {
		t.Fatal(e)
	}
	l, e = OpenWorkflowLog(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	p, e := l.ReadBatch(context.Background(), Partition{Topic: "transitions"}, 0, Limits{2, 32}, false)
	if e != nil || len(p.Records) != 1 || string(p.Records[0].Value) != "keep" {
		t.Fatal(p, e)
	}
}
