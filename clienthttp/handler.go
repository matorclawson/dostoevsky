// Package clienthttp exposes the public binary HTTP API for non-Go clients.
// Mount the handler on a Go application's HTTP server or use dostoevsky-gateway.
package clienthttp

import (
	"context"
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	d "github.com/rodriveracom/dostoevsky"
)

const Prefix = "/dostoevsky/client/v1/"

// New returns a handler for a Node or Mesh. The token grants access to the whole store.
func New(store d.Store, token string) (http.Handler, error) {
	if store == nil || len(token) < 16 {
		return nil, d.ErrInvalid
	}
	mux := http.NewServeMux()
	api := handler{store}
	mux.HandleFunc("POST "+Prefix+"publish", api.publish)
	mux.HandleFunc("GET "+Prefix+"read", api.read)
	mux.HandleFunc("GET "+Prefix+"position", api.position)
	mux.HandleFunc("POST "+Prefix+"commit", api.commit)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/octet-stream")
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			fail(w, d.ErrInvalid)
			return
		}
		for _, values := range query {
			if len(values) != 1 {
				fail(w, d.ErrInvalid)
				return
			}
		}
		r.Form = query
		mux.ServeHTTP(w, r)
	}), nil
}

type handler struct{ store d.Store }

func fail(w http.ResponseWriter, err error) {
	for _, v := range []struct {
		err  error
		code int
		name string
	}{
		{d.ErrInvalid, 400, "invalid"},
		{d.ErrOwner, 409, "wrong_owner"},
		{d.ErrConflict, 409, "conflict"},
		{d.ErrExpired, 410, "expired"},
		{d.ErrLimit, 413, "limit"},
		{d.ErrClosed, 503, "closed"},
		{context.DeadlineExceeded, 504, "timeout"},
		{context.Canceled, 408, "canceled"},
	} {
		if errors.Is(err, v.err) {
			http.Error(w, v.name, v.code)
			return
		}
	}
	http.Error(w, "storage_failure", 500)
}
func body(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, d.MaxValue))
	var over *http.MaxBytesError
	if errors.As(err, &over) {
		return nil, d.ErrLimit
	}
	if err != nil {
		return nil, d.ErrInvalid
	}
	return b, nil
}
func number(q url.Values, k string) (int64, error) {
	n, err := strconv.ParseInt(q.Get(k), 10, 64)
	if err != nil || n < 0 {
		return 0, d.ErrInvalid
	}
	return n, nil
}
func partition(q url.Values) (d.Partition, error) {
	n, err := number(q, "partition")
	if err != nil || n > 65535 {
		return d.Partition{}, d.ErrInvalid
	}
	return d.Partition{Topic: q.Get("topic"), Number: int(n)}, nil
}
func receipt(w http.ResponseWriter, r d.Record) {
	w.Header().Set("Dostoevsky-Partition", strconv.Itoa(r.Number))
	w.Header().Set("Dostoevsky-Offset", strconv.FormatInt(r.Offset, 10))
}
func (h handler) publish(w http.ResponseWriter, r *http.Request) {
	q := r.Form
	b, err := body(w, r)
	if err != nil {
		fail(w, err)
		return
	}
	rec, err := h.store.Publish(r.Context(), q.Get("topic"), d.Message{Key: q.Get("key"), ID: q.Get("id"), Value: b})
	if err != nil {
		fail(w, err)
		return
	}
	receipt(w, rec)
	w.WriteHeader(200)
}
func (h handler) read(w http.ResponseWriter, r *http.Request) {
	q := r.Form
	p, err := partition(q)
	if err != nil {
		fail(w, err)
		return
	}
	off, err := number(q, "offset")
	if err != nil {
		fail(w, err)
		return
	}
	var ms int64
	if q.Has("wait_ms") {
		ms, err = number(q, "wait_ms")
		if err != nil || ms > 30000 {
			fail(w, d.ErrInvalid)
			return
		}
	}
	ctx := r.Context()
	if ms > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
		defer cancel()
	}
	rec, err := h.store.Read(ctx, p, off, ms > 0)
	if errors.Is(err, d.ErrEmpty) || (ms > 0 && errors.Is(err, context.DeadlineExceeded) && ctx.Err() == context.DeadlineExceeded) {
		w.WriteHeader(204)
		return
	}
	if err != nil {
		fail(w, err)
		return
	}
	receipt(w, rec)
	w.Header().Set("Dostoevsky-Key", url.PathEscape(rec.Key))
	w.Header().Set("Dostoevsky-ID", url.PathEscape(rec.ID))
	w.Header().Set("Dostoevsky-Time", rec.Time.UTC().Format(time.RFC3339Nano))
	_, _ = w.Write(rec.Value)
}
func (h handler) position(w http.ResponseWriter, r *http.Request) {
	q := r.Form
	p, err := partition(q)
	if err != nil {
		fail(w, err)
		return
	}
	pos, err := h.store.Position(r.Context(), p, q.Get("group"))
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Dostoevsky-Next", strconv.FormatInt(pos.Next, 10))
	_, _ = w.Write(pos.State)
}
func (h handler) commit(w http.ResponseWriter, r *http.Request) {
	q := r.Form
	p, err := partition(q)
	if err != nil {
		fail(w, err)
		return
	}
	off, err := number(q, "expected")
	if err != nil {
		fail(w, err)
		return
	}
	state, err := body(w, r)
	if err == nil {
		err = h.store.Commit(r.Context(), p, q.Get("group"), off, state)
	}
	if err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}
