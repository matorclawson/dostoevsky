package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCapture(t *testing.T) {
	for _, tc := range []struct {
		mode, path, event string
		partial           bool
	}{{"agent", "/agents/sessions", "agent.session.turn.completed", false}, {"text", "/responses", "response.completed", false}, {"agent", "/agents/sessions", "agent.session.idle", true}} {
		t.Run(tc.event, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var b map[string]any
				if r.URL.Path != tc.path || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer test" || json.NewDecoder(r.Body).Decode(&b) != nil || b["stream"] != true {
					t.Error("invalid request")
				}
				if tc.mode == "agent" && (r.Header.Get("OpenAI-Beta") != "agents=v1" || b["environment"] == nil) {
					t.Error("invalid agent request")
				}
				fmt.Fprintf(w, ": keepalive\r\n\r\ndata: {\"type\":\r\ndata: %q}\r\n\r\n", tc.event)
			}))
			defer s.Close()
			count := 0
			e := capture(context.Background(), s.Client(), s.URL, tc.mode, "model", "test", func([]byte) error { count++; return nil })
			if count != 1 || tc.partial && !errors.Is(e, io.ErrUnexpectedEOF) || !tc.partial && e != nil {
				t.Fatal(count, e)
			}
		})
	}
}
