//! A string as Go's `%q` writes it (`strconv.Quote`). Go's error texts quote names this way, and the texts of this
//! engine's errors are Go's, so a name with a quote or a control character in it reads the same from both.
//!
//! The input is valid UTF-8. Go also has a form for a byte that is not (`\xNN`); the driver only quotes text that
//! the JSON reader has already made valid.

use crate::goquote_table::PRINT;

/// Whether Go prints the character as it is: `strconv.IsPrint`. In ASCII that is the space through the tilde.
/// Beyond ASCII it is letters, marks, numbers, punctuation and symbols, by the table Go itself wrote
/// (`goquote_table.rs`). The standard library's own notion of a printable character differs: it escapes combining
/// marks, which Go prints.
fn is_print(c: char) -> bool {
    if c.is_ascii() {
        return (' '..='~').contains(&c);
    }
    let c = c as u32;
    let after = PRINT.partition_point(|range| range.1 < c);
    PRINT.get(after).is_some_and(|range| range.0 <= c)
}

/// Go's `strconv.Quote(s)`.
pub fn quote(s: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            c if is_print(c) => out.push(c),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            c if c < ' ' || c == '\u{7f}' => write!(out, "\\x{:02x}", c as u32).unwrap(),
            c if (c as u32) < 0x10000 => write!(out, "\\u{:04x}", c as u32).unwrap(),
            c => write!(out, "\\U{:08x}", c as u32).unwrap(),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::quote;

    #[test]
    fn the_forms_by_name() {
        assert_eq!(quote("segment.write"), "\"segment.write\"");
        assert_eq!(quote("a\"b\\c"), r#""a\"b\\c""#);
        assert_eq!(quote("\u{7}\u{8}\u{c}\n\r\t\u{b}"), r#""\a\b\f\n\r\t\v""#);
        assert_eq!(
            quote("\0\u{1f}\u{7f}"),
            r#""\x00\x1f\x7f""#,
            "Rust's own form is \\0 and \\u{{1f}}"
        );
        assert_eq!(quote("\u{80}\u{2028}\u{feff}"), r#""\u0080\u2028\ufeff""#);
        assert_eq!(quote("\u{e0001}"), r#""\U000e0001""#);
        assert_eq!(quote("café 日本 😀 a\u{301}"), "\"café 日本 😀 a\u{301}\"");
    }
}
