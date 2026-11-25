package main

import (
	"encoding/base64"
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
