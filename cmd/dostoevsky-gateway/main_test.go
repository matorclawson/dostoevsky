package main

import (
	"encoding/base64"
	"net"
	"testing"
)

func TestCheckListenRefusesPlaintextOffLoopback(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8787", "[::1]:8787", "localhost:8787", "127.5.5.5:1"} {
		if err := checkListen(addr, false); err != nil {
			t.Errorf("%s refused: %v", addr, err)
		}
	}
	for _, addr := range []string{":8787", "0.0.0.0:8787", "[::]:8787", "10.0.0.5:8787", "example.com:8787", "gateway.internal:80"} {
		if err := checkListen(addr, false); err == nil {
			t.Errorf("%s accepted in plaintext", addr)
		}
		if err := checkListen(addr, true); err != nil {
			t.Errorf("%s refused with TLS: %v", addr, err)
		}
	}
	if err := checkListen("no-port", false); err == nil {
		t.Error("an address without a port was accepted")
	}
}

func TestNewTokenIsLongRandomAndUnique(t *testing.T) {
	a, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := newToken()
	raw, err := base64.RawURLEncoding.DecodeString(a)
	if err != nil || len(raw) != 32 || len(a) != 43 {
		t.Fatalf("token %q: %d bytes, err %v", a, len(raw), err)
	}
	if a == b {
		t.Fatal("two tokens were identical")
	}
}

func TestTheBoundAddressIsCheckedNotOnlyTheFlag(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:0", "localhost:0", "[::1]:0"} {
		ln, err := listen(addr, false)
		if err != nil {
			t.Logf("%s: %v (IPv6 may be unavailable)", addr, err)
			continue
		}
		if !ln.Addr().(*net.TCPAddr).IP.IsLoopback() {
			t.Errorf("%s bound to %v", addr, ln.Addr())
		}
		ln.Close()
	}
	for _, a := range []net.Addr{
		&net.TCPAddr{IP: net.IPv4zero, Port: 1}, &net.TCPAddr{IP: net.IPv6unspecified, Port: 1},
		&net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 1}, &net.UnixAddr{Name: "x", Net: "unix"},
	} {
		if checkBound(a, false) == nil {
			t.Errorf("%v accepted without TLS", a)
		}
		if checkBound(a, true) != nil {
			t.Errorf("%v refused with TLS", a)
		}
	}
	if err := checkBound(&net.TCPAddr{IP: net.ParseIP("127.9.9.9"), Port: 1}, false); err != nil {
		t.Error(err)
	}
}
