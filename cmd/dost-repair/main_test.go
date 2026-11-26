package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	d "github.com/rodriveracom/dostoevsky"
)

func TestInspectThenRepair(t *testing.T) {
	dir := t.TempDir()
	n, err := d.Open(dir, d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"one", "two"} {
		if _, err := n.Publish(context.Background(), "events", d.Message{Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}
	n.Close()
	path := filepath.Join(dir, "journal.v1")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-2] ^= 1
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		args []string
		code int
		want string
	}{
		{[]string{"-data", dir}, 1, "damage=checksum"},
		{[]string{"-data", dir, "-repair"}, 0, "repaired: removed"},
		{[]string{"-data", dir}, 0, "damage=none"},
		{[]string{"-data", filepath.Join(dir, "no")}, 2, "no journal"},
		{[]string{"-data", dir, "-discard-later-frames"}, 2, "usage"},
		{nil, 2, "usage"},
	} {
		var out, errOut bytes.Buffer
		if code := run(c.args, &out, &errOut); code != c.code || !strings.Contains(out.String()+errOut.String(), c.want) {
			t.Fatalf("%v: exit %d, %s%s", c.args, code, out.String(), errOut.String())
		}
	}
}
