// Command gateway runs the publisher workload against dostoevsky-gateway: the same
// durable engine as the dost command, but reached over HTTP/1.1 on loopback from a Go
// net/http client with keep-alive. It is the CONTROL for the per-language client
// benchmarks: the distance from dost to this command is the cost of HTTP alone, and any
// further drop in a language's client is that client's own cost. The gateway runs as a
// separate process whose memory is reported next to the harness's. The binary comes from
// BENCH_SERVER_BIN (go build ./cmd/dostoevsky-gateway).
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/servers"
)

const prefix = "/dostoevsky/client/v1/"

type store struct {
	client *http.Client
	base   string // http://127.0.0.1:port
	token  string
	proc   *serverproc.Proc // nil in tests that bring their own server
}

func newClient(workers int) *http.Client {
	return &http.Client{
		Timeout: 60 * time.Second,
		// Keep one connection per producer; never follow redirects.
		Transport: &http.Transport{
			MaxIdleConns: 4 * workers, MaxIdleConnsPerHost: 4 * workers, IdleConnTimeout: 90 * time.Second,
			DisableCompression: true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func (s store) do(method, path string, body []byte) (*http.Response, []byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, s.base+prefix+path, rd)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/octet-stream")
	res, err := s.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		return nil, nil, err
	}
	if res.StatusCode/100 != 2 {
		return nil, nil, fmt.Errorf("%s %s: %d %s", method, path, res.StatusCode, strings.TrimSpace(string(data[:min(len(data), 128)])))
	}
	return res, data, nil
}

func (s store) Publish(data []byte) (uint64, error) {
	res, _, err := s.do("POST", "publish?topic=events", data)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(res.Header.Get("Dostoevsky-Offset"), 10, 64)
}

func (s store) Read(i uint64) ([]byte, error) {
	res, data, err := s.do("GET", "read?topic=events&partition=0&wait_ms=0&offset="+strconv.FormatUint(i, 10), nil)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusNoContent {
		return nil, fmt.Errorf("offset %d: nothing there", i)
	}
	return data, nil
}

func (s store) ServerPeakKiB() (uint64, error) {
	if s.proc == nil {
		return 0, fmt.Errorf("no server process")
	}
	return s.proc.PeakKiB()
}

func (s store) Close() {
	s.client.CloseIdleConnections()
	if s.proc == nil {
		return
	}
	if err := s.proc.Stop(30 * time.Second); err != nil {
		panic(err)
	}
}

func main() {
	bin := os.Getenv("BENCH_SERVER_BIN")
	workers := 1
	for i, a := range os.Args { // the harness's own flag, read early to size the connection pool
		if a == "-workers" && i+1 < len(os.Args) {
			if n, err := strconv.Atoi(os.Args[i+1]); err == nil && n > 0 {
				workers = n
			}
		}
	}
	run.Main("gateway", func(dir string, _ bool) (run.Store, error) {
		// The same code that starts the gateway for the language benchmarks (internal/servers).
		// Start returns after an authorized read succeeded, which ends the timed restart:
		// reopen is the process start until the gateway answers a read.
		srv, err := servers.Start("gateway", servers.Options{Bin: bin, Dir: dir})
		if err != nil {
			return nil, err
		}
		return store{newClient(workers), "http://" + srv.Addr, srv.Token, srv.Proc}, nil
	})
}
