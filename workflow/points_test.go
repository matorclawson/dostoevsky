package workflow

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/rodriveracom/dostoevsky/internal/seam"
)

// The workflow commit points: each fails the operation, and after reopening the operation is there only when the point
// lies after the witness was replaced.
func TestEveryWorkflowCommitPoint(t *testing.T) {
	steps := []Step{{Kind: "activity", Name: "send", Attempts: 1}}
	for _, c := range []struct {
		point   string
		present bool
	}{
		{"workflow.before_append", false},
		{"workflow.mid_append", false},
		{"workflow.after_segment_sync_before_witness", false},
		{"workflow.after_witness_before_reply", true},
	} {
		t.Run(c.point, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := seam.Arm(c.point, false, 0); err != nil {
				t.Fatal(err)
			}
			defer seam.Disarm()
			err = s.Register("work", 1, steps)
			if !seam.Disarm() {
				t.Fatalf("the point was not reached (Register returned %v)", err)
			}
			if !errors.Is(err, seam.ErrInjected) {
				t.Fatalf("Register: %v", err)
			}
			_ = s.Close()
			s, err = Open(dir)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer s.Close()
			_, err = s.StartOnce("run", "work", 1, "q", []byte("0"), nil)
			if present := err == nil; present != c.present {
				t.Fatalf("the registration is present: %v (StartOnce: %v), want %v", present, err, c.present)
			}
		})
	}
}

// A skipped pass of the mid-append point writes nothing of its own: the first commit succeeds and survives, the second
// is cut off.
func TestMidAppendWritesNothingOnASkippedPass(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := seam.Arm("workflow.mid_append", false, 1); err != nil {
		t.Fatal(err)
	}
	defer seam.Disarm()
	if err := s.Register("work", 1, []Step{{Kind: "activity", Name: "send", Attempts: 1}}); err != nil {
		t.Fatalf("the skipped pass failed: %v", err)
	}
	if _, err := s.StartOnce("run", "work", 1, "q", []byte("0"), nil); !errors.Is(err, seam.ErrInjected) {
		t.Fatalf("the second commit: %v", err)
	}
	if !seam.Disarm() {
		t.Fatal("the point was not reached")
	}
	_ = s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	if created, err := s.StartOnce("run", "work", 1, "q", []byte("0"), nil); err != nil || !created {
		t.Fatalf("after reopen: created %v, %v", created, err)
	}
}
