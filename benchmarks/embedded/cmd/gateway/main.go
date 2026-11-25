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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
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
		if bin == "" {
			return nil, fmt.Errorf("BENCH_SERVER_BIN is not set")
		}
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			return nil, err
		}
		token := hex.EncodeToString(raw)
		port, err := serverproc.FreePort()
		if err != nil {
			return nil, err
		}
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		proc, err := serverproc.Start(bin, []string{"-listen", addr, "-data", filepath.Join(dir, "data")},
			append(os.Environ(), "DOSTOEVSKY_TOKEN="+token), filepath.Join(dir, "server.log"), 50*time.Millisecond)
		if err != nil {
			return nil, err
		}
		if err = proc.WaitReady("tcp", addr, 60*time.Second); err != nil {
			proc.Stop(5 * time.Second)
			return nil, err
		}
		s := store{newClient(workers), "http://" + addr, token, proc}
		// The first successful data request ends the timed restart: reopen is the process
		// start until the gateway answers a read (204 on an empty log, 200 otherwise).
		if _, _, err = s.do("GET", "read?topic=events&partition=0&offset=0&wait_ms=0", nil); err != nil {
			s.Close()
			return nil, err
		}
		return s, nil
	})
}
