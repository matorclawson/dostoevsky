// Package servers starts the engine servers the benchmarks talk to, one per trial, with
// the durability settings listed in benchmarks/comparison/ENGINES.md. The Go comparison,
// the gateway control and the per-language client benchmarks all start their servers
// here, so no two of them can drift apart. Each server gets a private loopback port and a
// random token, and the process answering is checked to be the launched one.
package servers

import (
	"bufio"
	"context"
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

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
)

// RedisDurability is the persistence part of every Redis and Valkey configuration:
// the append-only file synced on every write, no snapshots.
const RedisDurability = "appendonly yes\nappendfsync always\nsave \"\"\n"

// Server is a running engine server.
type Server struct {
	Engine string
	Addr   string // host:port on loopback
	Token  string // secret for this trial only
	Proc   *serverproc.Proc
	name   string // nats: the server_name this trial's configuration carries (not derived from the token)
}

// Options select the binary and where the trial's files go.
type Options struct {
	Ctx    context.Context // cancels the start: the server is stopped and Start returns (default: never)
	Bin    string          // the server executable
	Env    []string        // extra environment for the server process (for example GOMAXPROCS)
	Dir    string          // private directory for this trial
	Sample time.Duration   // memory sampling interval (default 50 ms)
}

// Engines lists what Start accepts.
var Engines = []string{"gateway", "redis", "nats"}

// Start launches the engine's server and returns once it answers as the launched process.
func Start(engine string, o Options) (*Server, error) {
	if o.Ctx == nil {
		o.Ctx = context.Background()
	}
	if o.Bin == "" {
		return nil, fmt.Errorf("%s: no server binary given", engine)
	}
	if o.Sample == 0 {
		o.Sample = 50 * time.Millisecond
	}
	port, err := serverproc.FreePort()
	if err != nil {
		return nil, err
	}
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(raw)
	nameRaw := make([]byte, 8) // a separate random value: the server's name is not derived from its secret
	if _, err = rand.Read(nameRaw); err != nil {
		return nil, err
	}
	name := "bench-" + hex.EncodeToString(nameRaw)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	var args, env []string
	switch engine {
	case "gateway":
		args = []string{"-listen", addr, "-data", filepath.Join(o.Dir, "data")}
		env = append([]string{"DOSTOEVSKY_TOKEN=" + token}, o.Env...)
	case "redis":
		conf := filepath.Join(o.Dir, "server.conf")
		if err = os.WriteFile(conf, []byte(RedisConfig(o.Dir, port, token)), 0o600); err != nil {
			return nil, err
		}
		args = []string{conf}
	case "nats":
		conf := filepath.Join(o.Dir, "server.conf")
		if err = os.WriteFile(conf, []byte(NATSConfig(o.Dir, port, token, name)), 0o600); err != nil {
			return nil, err
		}
		args = []string{"-c", conf}
	default:
		return nil, fmt.Errorf("unknown engine %q (have %s)", engine, strings.Join(Engines, ", "))
	}
	proc, err := serverproc.Start(o.Bin, args, env, filepath.Join(o.Dir, "server.log"), o.Sample)
	if err != nil {
		return nil, err
	}
	s := &Server{Engine: engine, Addr: addr, Token: token, Proc: proc, name: name}
	// Readiness runs beside a cancellation: if the driver is told to stop while the server is
	// still starting, the server is stopped here and nothing is left running.
	ready := make(chan error, 1)
	go func() {
		e := proc.WaitReady("tcp", addr, 60*time.Second)
		if e == nil {
			e = s.confirm(o.Ctx)
		}
		ready <- e
	}()
	select {
	case err = <-ready:
	case <-o.Ctx.Done():
		err = fmt.Errorf("start cancelled: %w", o.Ctx.Err())
	}
	if err != nil {
		proc.Stop(5 * time.Second)
		return nil, fmt.Errorf("%s: %w", engine, err)
	}
	proc.Sample() // a reading right after readiness, whatever the ticker does next
	return s, nil
}

// Stop ends the server and its children and reports whether it stopped cleanly.
func (s *Server) Stop() error { return s.Proc.Stop(30 * time.Second) }

// RedisConfig is one trial's redis.conf: loopback TCP on its own port, a password, and
// the shared durability settings.
func RedisConfig(dir string, port int, token string) string {
	return fmt.Sprintf("port %d\nbind 127.0.0.1\nprotected-mode yes\nrequirepass %s\ndir %q\n", port, token, dir) + RedisDurability
}

// NATSConfig is one trial's nats-server configuration. sync_interval: always is the
// file form of the SyncAlways option the Go comparison sets (nats-server v2.15.3,
// server/opts.go: case "sync", "sync_interval": "always" sets opts.SyncAlways).
func NATSConfig(dir string, port int, token, name string) string {
	return fmt.Sprintf("listen: 127.0.0.1:%d\nserver_name: %q\nauthorization { token: %q }\njetstream { store_dir: %q\n sync_interval: always }\n",
		port, name, token, filepath.Join(dir, "nats"))
}

// confirm checks that the process answering on Addr is the launched server, by a
// secret only that process was given.
func (s *Server) confirm(ctx context.Context) error {
	switch s.Engine {
	case "gateway":
		req, err := http.NewRequestWithContext(ctx, "GET", "http://"+s.Addr+"/dostoevsky/client/v1/read?topic=events&partition=0&offset=0&wait_ms=0", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+s.Token)
		c := http.Client{Timeout: 10 * time.Second}
		res, err := c.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		io.Copy(io.Discard, res.Body)
		if res.StatusCode != 200 && res.StatusCode != 204 {
			return fmt.Errorf("the gateway answered %d to an authorized read", res.StatusCode)
		}
	case "redis":
		c, err := net.DialTimeout("tcp", s.Addr, 10*time.Second)
		if err != nil {
			return err
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err = fmt.Fprintf(c, "AUTH %s\r\nPING\r\n", s.Token); err != nil {
			return err
		}
		r := bufio.NewReader(c)
		for _, want := range []string{"+OK", "+PONG"} {
			line, err := r.ReadString('\n')
			if err != nil || strings.TrimSpace(line) != want {
				return fmt.Errorf("redis answered %q (%v), wanted %s", line, err, want)
			}
		}
	case "nats":
		c, err := net.DialTimeout("tcp", s.Addr, 10*time.Second)
		if err != nil {
			return err
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(10 * time.Second))
		line, err := bufio.NewReader(c).ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "INFO ") || !strings.Contains(line, `"server_name":"`+s.name+`"`) {
			return fmt.Errorf("the process answering is not the launched nats-server: %q (%v)", line, err)
		}
	}
	return nil
}
