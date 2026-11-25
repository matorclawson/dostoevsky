// Package stubserver is a stand-in server for the self-tests of the server driver: it
// answers just enough of each engine's protocol for readiness checks. Tests run the test
// binary itself as the stub (see Main).
package stubserver

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Main runs the stub when SERVERS_STUB is set and never returns; otherwise it returns.
func Main() {
	engine := os.Getenv("SERVERS_STUB")
	if engine == "" {
		return
	}
	args := os.Args[1:]
	switch engine {
	case "gateway":
		var addr string
		for i, a := range args {
			if a == "-listen" {
				addr = args[i+1]
			}
		}
		token := os.Getenv("DOSTOEVSKY_TOKEN")
		fail(http.ListenAndServe(addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(401)
				return
			}
			w.WriteHeader(204)
			if os.Getenv("SERVERS_STUB_EXIT") != "" {
				go func() { time.Sleep(50 * time.Millisecond); os.Exit(0) }()
			}
		})))
	case "redis":
		conf := read(args[0])
		serve(field(conf, "port"), func(c net.Conn) {
			sc := bufio.NewScanner(c)
			for sc.Scan() {
				switch line := strings.TrimSpace(sc.Text()); {
				case line == "AUTH "+field(conf, "requirepass"):
					fmt.Fprint(c, "+OK\r\n")
				case line == "PING":
					fmt.Fprint(c, "+PONG\r\n")
				}
			}
		})
	case "nats":
		conf := read(args[1])
		name := strings.Trim(field(conf, "server_name:"), `"`)
		listen := field(conf, "listen:")
		_, port, _ := net.SplitHostPort(listen)
		serve(port, func(c net.Conn) { fmt.Fprintf(c, "INFO {\"server_name\":\"%s\"}\r\n", name) })
	}
	os.Exit(0)
}

func read(path string) string {
	b, err := os.ReadFile(path)
	fail(err)
	return string(b)
}

// field returns the text after key up to the end of its line.
func field(conf, key string) string {
	for _, line := range strings.Split(conf, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), key); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func serve(port string, handle func(net.Conn)) {
	l, err := net.Listen("tcp", "127.0.0.1:"+port)
	fail(err)
	for n := 1; ; n++ {
		c, err := l.Accept()
		if err != nil {
			os.Exit(1)
		}
		// SERVERS_STUB_EXIT: a server that stops at once. Connection 1 is the driver's readiness
		// probe, connection 2 its identity check; the stub answers that one and is gone.
		if os.Getenv("SERVERS_STUB_EXIT") != "" && n == 2 {
			handle(c)
			c.Close()
			os.Exit(0)
		}
		go func() { defer c.Close(); handle(c) }()
	}
}

func fail(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "stub:", err)
		os.Exit(1)
	}
}
