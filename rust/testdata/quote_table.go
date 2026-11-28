//go:build ignore

// Writes the table of characters that Go's %q prints as they are, for the Rust form of strconv.Quote:
//
//	go run rust/testdata/quote_table.go > rust/crates/dostoevsky-format/src/goquote_table.rs
//
// The table is strconv.IsPrint for every character beyond ASCII, as ranges. It follows the Unicode tables of the Go
// release that writes it, so it is written again together with goquote-go.txt.
package main

import (
	"fmt"
	"runtime"
	"strconv"
	"unicode"
)

func main() {
	fmt.Printf("//! Written by %s with rust/testdata/quote_table.go; do not edit.\n", runtime.Version())
	fmt.Printf("//! The characters beyond ASCII that Go's `strconv.IsPrint` accepts, as inclusive ranges in order.\n\n")
	fmt.Printf("#[rustfmt::skip]\npub(crate) const PRINT: &[(u32, u32)] = &[\n")
	start := rune(-1)
	for r := rune(0x80); r <= unicode.MaxRune+1; r++ {
		printable := r <= unicode.MaxRune && !(r >= 0xd800 && r <= 0xdfff) && strconv.IsPrint(r)
		if printable && start < 0 {
			start = r
		}
		if !printable && start >= 0 {
			fmt.Printf("    (0x%x, 0x%x),\n", start, r-1)
			start = -1
		}
	}
	fmt.Printf("];\n")
}
