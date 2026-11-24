package main

import (
	"strings"
	"testing"
)

func TestConfigIsPrivateToTheTrial(t *testing.T) {
	conf, sock := serverConfig("/data/trial-1")
	if !strings.Contains(conf, "port 0\n") || !strings.Contains(conf, "unixsocket /data/trial-1/server.sock\n") || sock != "/data/trial-1/server.sock" {
		t.Fatalf("server is not private to the trial:\n%s", conf)
	}
	if strings.Contains(conf, "6390") || strings.Contains(conf, "bind ") {
		t.Fatalf("a fixed TCP address is configured:\n%s", conf)
	}
	for _, want := range []string{"appendonly yes\n", "appendfsync always\n"} {
		if !strings.Contains(conf, want) {
			t.Fatalf("missing durability setting %q", want)
		}
	}
}

// An ambiguous XADD must never be retried with a new generated ID.
func TestClientDoesNotRetryCommands(t *testing.T) {
	o := clientOptions("/x.sock")
	if o.MaxRetries != -1 || o.Network != "unix" || o.Addr != "/x.sock" {
		t.Fatalf("options: retries %d, network %q, addr %q", o.MaxRetries, o.Network, o.Addr)
	}
}

func TestStreamIDsRoundTripThroughTheHarnessOffset(t *testing.T) {
	for _, id := range []string{"1700000000000-0", "1700000000123-7", "5-65535"} {
		v, err := pack(id)
		if err != nil {
			t.Fatal(err)
		}
		if got := unpack(v); got != id {
			t.Fatalf("%s came back as %s", id, got)
		}
	}
	if _, err := pack("1-65536"); err == nil {
		t.Fatal("a sequence that does not fit 16 bits was accepted")
	}
}

func TestServerPidIsParsedFromInfo(t *testing.T) {
	pid, err := serverPid("# Server\r\nredis_version:8.12.0\r\nprocess_id:4242\r\ntcp_port:0\r\n")
	if err != nil || pid != 4242 {
		t.Fatalf("pid %d, %v", pid, err)
	}
	if _, err := serverPid("# Server\r\n"); err == nil {
		t.Fatal("missing process_id accepted")
	}
}
