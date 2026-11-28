package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rodriveracom/dostoevsky/internal/seam"
)

// The runner refuses a second arm before it reaches a driver, so the driver's own answer, which another implementation
// must give too, is checked here through the line protocol.
func TestTheDriverRefusesToArmOverAnUnreportedPoint(t *testing.T) {
	defer seam.Disarm()
	var out bytes.Buffer
	requests := strings.Join([]string{
		`{"op":"arm","point":"no.such.point"}`,
		`{"op":"arm","point":"journal.write"}`,
		`{"op":"arm","point":"backup.write"}`,
		`{"op":"arm","point":"no.such.point"}`,
		`{"op":"disarm"}`,
		`{"op":"arm","point":"backup.write"}`,
	}, "\n") + "\n"
	if err := serve(strings.NewReader(requests), &out); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var r struct {
			OK      bool
			Error   string
			Message string
		}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		if r.OK {
			got = append(got, "ok")
			continue
		}
		got = append(got, r.Error+": "+r.Message)
	}
	want := []string{
		`other: seam: no point is named "no.such.point"`,
		"ok",
		`other: seam: point "journal.write" is armed and disarm has not reported it`,
		`other: seam: no point is named "no.such.point"`,
		"ok",
		"ok",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("replies:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
