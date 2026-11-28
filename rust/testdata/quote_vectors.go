//go:build ignore

// Writes the vectors that check the Rust form of Go's %q against strconv.Quote:
//
//	go run rust/testdata/quote_vectors.go > rust/testdata/goquote-go.txt
//
// One row per string: the string and its quoted form, both in hex. The strings are valid UTF-8, because the driver
// only quotes text that the JSON decoder has already made valid. They are every character up to U+024F one by one,
// characters of each kind strconv treats differently, and a few longer strings. Characters assigned or reclassified
// in recent Unicode versions are left out, so that the rows do not depend on the tables of one release.
package main

import (
	"encoding/hex"
	"fmt"
	"runtime"
	"strconv"
)

func main() {
	fmt.Printf("# written by %s with rust/testdata/quote_vectors.go; do not edit\n", runtime.Version())
	var inputs []string
	for r := rune(0); r <= 0x24f; r++ {
		inputs = append(inputs, string(r))
	}
	for _, r := range []rune{0x300, 0x301, 0x391, 0x3a9, 0x430, 0x5d0, 0x627, 0x64b, 0x93f, 0xe01, 0x1100, 0x2000, 0x2003, 0x200b, 0x200d, 0x200e, 0x2028, 0x2029,
		0x202e, 0x2060, 0x20ac, 0x2122, 0x212a, 0x2190, 0x3000, 0x3042, 0x4e2d, 0xac00, 0xd7a3, 0xe000, 0xf8ff, 0xfb01, 0xfe0f, 0xfeff, 0xfff9, 0xfffc, 0xfffd,
		0xfffe, 0xffff, 0x10000, 0x1d11e, 0x1f600, 0x1f1e6, 0x1fffe, 0x1ffff, 0x20000, 0xe0001, 0xe0100, 0xf0000, 0x10fffd, 0x10ffff} {
		inputs = append(inputs, string(r))
	}
	inputs = append(inputs, "", "segment.write", "no.such.point", "a b", "a\"b\\c", "tab\there", "line\nend\r\n", "\x00\x01\x1f\x7f", "café", "\u00a0x\u00ad",
		"á", "́a", "日本語", "x\u2028y\u2029z", string(rune(0xfeff))+"bom", "\ufffd\ufffd", "wf_\a\b\f\v", "😀 ok", "'single'", "`back`", "<&>")
	for _, s := range inputs {
		fmt.Printf("-%s- -%s-\n", hex.EncodeToString([]byte(s)), hex.EncodeToString([]byte(strconv.Quote(s))))
	}
}
