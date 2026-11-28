package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

func open(t *testing.T, dir string) *workflow.Store {
	t.Helper()
	s, err := workflow.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = register(s); err != nil {
		t.Fatal(err)
	}
	return s
}

var secret = []byte("a test secret of some length")

func post(t *testing.T, s *workflow.Store, body string, want int) {
	t.Helper()
	send(t, s, body, sign(secret, []byte(body)), want)
}

func send(t *testing.T, s *workflow.Store, body, signature string, want int) {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	r.Header.Set("X-Signature", signature)
	intake(s, secret)(w, r)
	if w.Code != want {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestCrashAndResume(t *testing.T) {
	dir := t.TempDir()
	applied := map[string]string{}
	apply := func(key string, e event) error {
		applied[key] = e.Type
		return nil
	}
	const body = `{"id":"e1","type":"paid","invoice":"i1"}`

	s := open(t, dir)
	send(t, s, body, "", http.StatusUnauthorized)
	send(t, s, body, sign([]byte("another secret"), []byte(body)), http.StatusUnauthorized)
	if task, err := s.Poll(queue, time.Minute); err != nil || task != nil {
		t.Fatal("unsigned event stored", task, err)
	}
	post(t, s, body, http.StatusOK)
	s.Close()

	s = open(t, dir)
	post(t, s, ` {"invoice":"i1", "type":"paid", "id":"e1", "extra":1}`, http.StatusOK)
	post(t, s, `{"id":"e1","type":"failed","invoice":"i1"}`, http.StatusConflict)
	post(t, s, `{}`, http.StatusBadRequest)
	post(t, s, body+`{}`, http.StatusBadRequest)
	post(t, s, `{"id":"`+strings.Repeat("x", 300)+`","invoice":"i1"}`, http.StatusBadRequest)
	post(t, s, body+strings.Repeat(" ", bodyLimit), http.StatusRequestEntityTooLarge)
	// Inside the wire limit, but six bytes per character once re-encoded.
	post(t, s, `{"id":"e2","invoice":"i1","type":"`+strings.Repeat("<", 30000)+`"}`, http.StatusRequestEntityTooLarge)

	// Die after the effect and before Finish.
	task, err := s.Poll(queue, 50*time.Millisecond)
	if err != nil || task == nil {
		t.Fatal(task, err)
	}
	if _, failure := handle(task, apply); failure != "" || applied["payment-event/e1"] != "paid" {
		t.Fatal("first effect", failure, applied)
	}
	lost := task.Run
	s.Close()
	time.Sleep(100 * time.Millisecond)

	s = open(t, dir)
	task, err = s.Poll(queue, time.Minute)
	if err != nil || task == nil || task.Run != lost || task.Attempt != 2 {
		t.Fatal(task, err)
	}
	out, failure := handle(task, apply)
	if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s = open(t, dir)
	defer s.Close()
	post(t, s, body, http.StatusOK)
	if task, err = s.Poll(queue, time.Minute); err != nil || task != nil {
		t.Fatal(task, err)
	}
	if len(applied) != 1 || applied["payment-event/e1"] != "paid" {
		t.Fatal(applied)
	}

	// A store that cannot commit must not acknowledge.
	s.Close()
	post(t, s, body, http.StatusServiceUnavailable)
}
