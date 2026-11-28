//! Frame format version 3 of the segmented log. See frame3.go and checkpoint3.go: this file
//! implements the same bytes, and the conformance run checks that each engine opens the other's
//! data.
//!
//!   0   4  length of the body (u32, little-endian)
//!   4   1  type
//!   5   1  flags (zero)
//!   6   4  header check: CRC-32C of bytes 0 to 5
//!   10  4  body check: CRC-32C of the body, seeded with the header check
//!   14  .. body

use crate::crc32c;

pub const HEADER: usize = 14;
pub const MAX_BODY: usize = 4 << 20;

pub const T_RECORD: u8 = 1;
pub const T_COMMIT: u8 = 2;
pub const T_CP_BEGIN: u8 = 3;
pub const T_TOPIC_DEF: u8 = 4;
pub const T_FENCE: u8 = 5;
pub const T_CONFIG: u8 = 6;
pub const T_CP_PARTITION: u8 = 7;
pub const T_CP_GROUP: u8 = 8;
pub const T_CP_END: u8 = 9;

#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    /// The data ends inside the frame (a torn append when at the end of the active segment).
    Short,
    /// The header check failed: corruption, never a torn tail.
    Header,
    Checksum,
    Invalid(&'static str),
}

/// Opens a frame in `dst`; the caller appends the body and calls `seal`.
#[inline]
pub fn begin(dst: &mut Vec<u8>, typ: u8) -> usize {
    let start = dst.len();
    dst.extend_from_slice(&[0, 0, 0, 0, typ, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    start
}

#[inline]
pub fn seal(dst: &mut [u8], start: usize) {
    let body = dst.len() - start - HEADER;
    dst[start..start + 4].copy_from_slice(&(body as u32).to_le_bytes());
    let hcrc = crc32c::checksum(&dst[start..start + 6]);
    dst[start + 6..start + 10].copy_from_slice(&hcrc.to_le_bytes());
    let bcrc = crc32c::update(hcrc, &dst[start + HEADER..]);
    dst[start + 10..start + 14].copy_from_slice(&bcrc.to_le_bytes());
}

pub fn append(dst: &mut Vec<u8>, typ: u8, body: &[u8]) {
    let s = begin(dst, typ);
    dst.extend_from_slice(body);
    seal(dst, s);
}

/// Checks the frame at the start of `b`: (type, body, total size).
pub fn read(b: &[u8]) -> Result<(u8, &[u8], usize), FrameError> {
    if b.len() < HEADER {
        return Err(FrameError::Short);
    }
    let hcrc = crc32c::checksum(&b[..6]);
    if hcrc != u32::from_le_bytes(b[6..10].try_into().unwrap()) {
        return Err(FrameError::Header);
    }
    let n = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
    if n > MAX_BODY {
        return Err(FrameError::Invalid("length"));
    }
    if b[5] != 0 {
        return Err(FrameError::Invalid("flags"));
    }
    let size = HEADER + n;
    if b.len() < size {
        return Err(FrameError::Short);
    }
    if crc32c::update(hcrc, &b[HEADER..size]) != u32::from_le_bytes(b[10..14].try_into().unwrap()) {
        return Err(FrameError::Checksum);
    }
    Ok((b[4], &b[HEADER..size], size))
}

pub const ZERO_TIME: i64 = i64::MIN;

pub struct Record<'a> {
    pub partition: u32,
    pub offset: u64,
    pub time: i64,
    pub key: &'a [u8],
    pub id: &'a [u8],
    pub value: &'a [u8],
}

pub fn append_record(dst: &mut Vec<u8>, r: &Record) {
    dst.reserve(HEADER + 24 + r.key.len() + r.id.len() + r.value.len());
    let s = begin(dst, T_RECORD);
    dst.extend_from_slice(&r.partition.to_le_bytes());
    dst.extend_from_slice(&r.offset.to_le_bytes());
    dst.extend_from_slice(&r.time.to_le_bytes());
    dst.extend_from_slice(&(r.key.len() as u16).to_le_bytes());
    dst.extend_from_slice(&(r.id.len() as u16).to_le_bytes());
    dst.extend_from_slice(r.key);
    dst.extend_from_slice(r.id);
    dst.extend_from_slice(r.value);
    seal(dst, s);
}

pub fn parse_record(body: &[u8]) -> Result<Record<'_>, FrameError> {
    if body.len() < 24 {
        return Err(FrameError::Invalid("record body"));
    }
    let k = u16::from_le_bytes(body[20..22].try_into().unwrap()) as usize;
    let id = u16::from_le_bytes(body[22..24].try_into().unwrap()) as usize;
    let rest = &body[24..];
    if k + id > rest.len() {
        return Err(FrameError::Invalid("record key and id"));
    }
    Ok(Record {
        partition: u32::from_le_bytes(body[..4].try_into().unwrap()),
        offset: u64::from_le_bytes(body[4..12].try_into().unwrap()),
        time: i64::from_le_bytes(body[12..20].try_into().unwrap()),
        key: &rest[..k],
        id: &rest[k..k + id],
        value: &rest[k + id..],
    })
}

pub fn append_topic_def(dst: &mut Vec<u8>, partition: u32, number: u32, topic: &[u8]) {
    let s = begin(dst, T_TOPIC_DEF);
    dst.extend_from_slice(&partition.to_le_bytes());
    dst.extend_from_slice(&number.to_le_bytes());
    dst.extend_from_slice(topic);
    seal(dst, s);
}

pub struct Commit<'a> {
    pub partition: u32,
    pub group: &'a [u8],
    pub next: u64,
    pub expected: Option<u64>,
    pub state: &'a [u8],
}

pub fn append_commit(dst: &mut Vec<u8>, c: &Commit) {
    let s = begin(dst, T_COMMIT);
    dst.extend_from_slice(&c.partition.to_le_bytes());
    dst.extend_from_slice(&(c.group.len() as u16).to_le_bytes());
    dst.extend_from_slice(c.group);
    dst.extend_from_slice(&c.next.to_le_bytes());
    dst.push(c.expected.is_some() as u8);
    dst.extend_from_slice(&c.expected.unwrap_or(0).to_le_bytes());
    dst.extend_from_slice(c.state);
    seal(dst, s);
}

pub fn parse_commit(body: &[u8]) -> Result<Commit<'_>, FrameError> {
    if body.len() < 6 {
        return Err(FrameError::Invalid("commit body"));
    }
    let g = u16::from_le_bytes(body[4..6].try_into().unwrap()) as usize;
    let rest = &body[6..];
    if rest.len() < g + 17 {
        return Err(FrameError::Invalid("commit group"));
    }
    let (group, rest) = rest.split_at(g);
    let next = u64::from_le_bytes(rest[..8].try_into().unwrap());
    let e = u64::from_le_bytes(rest[9..17].try_into().unwrap());
    let expected = match rest[8] {
        0 if e == 0 => None,
        1 => Some(e),
        _ => return Err(FrameError::Invalid("commit expected-offset flag")),
    };
    Ok(Commit { partition: u32::from_le_bytes(body[..4].try_into().unwrap()), group, next, expected, state: &rest[17..] })
}
