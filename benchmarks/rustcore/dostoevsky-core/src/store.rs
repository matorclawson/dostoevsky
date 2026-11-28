use crate::crc32c;
use crate::frame::{self, FrameError};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Error, ErrorKind};
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MARKER: &[u8] = b"DOSTOEVSKY-SEGMENTED-01\n";
const SEG_HEADER: usize = 16;
const SEG_SIGNATURE: &[u8] = b"DOST0003";
const SEG_VERSION: u16 = 3;
const SEGMENT_TARGET: u64 = 8 << 20;
const SPARSE_STRIDE: u64 = 16;
const MAX_QUEUED: usize = 64;
const MAX_QUEUED_BYTES: usize = 1 << 20;
const GATHER_LIMIT: Duration = Duration::from_micros(100);
const INDEX_CACHE: usize = 32;

fn corrupt(seg: u64, at: u64, why: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::InvalidData, format!("the log is corrupt: segment {seg} at offset {at}: {why}"))
}

fn invalid(why: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, why.into())
}

fn seg_name(id: u64) -> String {
    format!("seg-{id:016x}.log")
}

fn parse_seg_name(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("seg-")?.strip_suffix(".log")?;
    if hex.len() != 16 || !hex.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)) {
        return None;
    }
    u64::from_str_radix(hex, 16).ok().filter(|&id| id != 0)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn seg_header() -> Vec<u8> {
    let mut h = Vec::with_capacity(SEG_HEADER);
    h.extend_from_slice(SEG_SIGNATURE);
    h.extend_from_slice(&SEG_VERSION.to_le_bytes());
    h.extend_from_slice(&0u16.to_le_bytes());
    let c = crc32c::checksum(&h);
    h.extend_from_slice(&c.to_le_bytes());
    h
}

fn check_seg_header(h: &[u8]) -> Result<(), &'static str> {
    if h.len() < SEG_HEADER || &h[..8] != SEG_SIGNATURE {
        return Err("not a segment header");
    }
    if u32::from_le_bytes(h[12..16].try_into().unwrap()) != crc32c::checksum(&h[..12]) {
        return Err("segment header check failed");
    }
    if u16::from_le_bytes(h[8..10].try_into().unwrap()) != SEG_VERSION || h[10] != 0 || h[11] != 0 {
        return Err("segment format version or flags");
    }
    Ok(())
}

fn range_bytes(first: u64, last: u64) -> [u8; 20] {
    let mut b = [0u8; 20];
    b[..8].copy_from_slice(&first.to_le_bytes());
    b[8..16].copy_from_slice(&last.to_le_bytes());
    let c = crc32c::checksum(&b[..16]);
    b[16..].copy_from_slice(&c.to_le_bytes());
    b
}

// ----- the durable state a checkpoint carries -----

#[derive(Clone, Default)]
struct PartState {
    base: u64,
    next: u64,
    groups: BTreeMap<Vec<u8>, (u64, Vec<u8>)>,
}

#[derive(Clone, Default)]
struct LogState {
    config: Vec<u8>,
    by_pid: BTreeMap<u32, (Vec<u8>, u32)>,
    by_part: HashMap<(Vec<u8>, u32), u32>,
    parts: BTreeMap<u32, PartState>,
    next_pid: u32,
}

impl LogState {
    fn define(&mut self, pid: u32, topic: &[u8], number: u32) -> Result<(), &'static str> {
        if let Some(old) = self.by_pid.get(&pid) {
            return if old.0 == topic && old.1 == number { Ok(()) } else { Err("partition id redefined") };
        }
        let key = (topic.to_vec(), number);
        if self.by_part.contains_key(&key) {
            return Err("partition defined under two ids");
        }
        self.by_pid.insert(pid, key.clone());
        self.by_part.insert(key, pid);
        self.parts.insert(pid, PartState::default());
        if pid >= self.next_pid {
            self.next_pid = pid + 1;
        }
        Ok(())
    }

    fn apply_record(&mut self, pid: u32, offset: u64) -> Result<(), &'static str> {
        let ps = self.parts.get_mut(&pid).ok_or("record for an undefined partition id")?;
        if offset != ps.next {
            return Err("record offset is not the partition's next offset");
        }
        ps.next += 1;
        Ok(())
    }

    fn apply_commit(&mut self, c: &frame::Commit) -> Result<(), &'static str> {
        let ps = self.parts.get_mut(&c.partition).ok_or("commit for an undefined partition id")?;
        if c.next == 0 && c.expected.is_none() {
            return Err("commit to offset 0 without an expected offset");
        }
        let expected = c.expected.unwrap_or(c.next.wrapping_sub(1));
        let at = ps.groups.get(c.group).map_or(0, |g| g.0);
        if expected != at || c.next <= expected || c.next > ps.next {
            return Err("commit does not follow the group's position");
        }
        ps.groups.insert(c.group.to_vec(), (c.next, c.state.to_vec()));
        Ok(())
    }

    /// The frames of one checkpoint, in the canonical order the Go reader requires.
    fn checkpoint(&self, id: u64, segment: u64, dst: &mut Vec<u8>) {
        let groups: usize = self.parts.values().map(|p| p.groups.len()).sum();
        let counts = |b: &mut Vec<u8>| {
            b.extend_from_slice(&(self.by_pid.len() as u32).to_le_bytes());
            b.extend_from_slice(&(self.parts.len() as u32).to_le_bytes());
            b.extend_from_slice(&(groups as u32).to_le_bytes());
        };
        let s = frame::begin(dst, frame::T_CP_BEGIN);
        dst.extend_from_slice(&id.to_le_bytes());
        dst.extend_from_slice(&segment.to_le_bytes());
        counts(dst);
        frame::seal(dst, s);
        frame::append(dst, frame::T_CONFIG, &self.config);
        for (pid, (topic, number)) in &self.by_pid {
            frame::append_topic_def(dst, *pid, *number, topic);
        }
        for (pid, ps) in &self.parts {
            let s = frame::begin(dst, frame::T_CP_PARTITION);
            dst.extend_from_slice(&pid.to_le_bytes());
            dst.extend_from_slice(&ps.base.to_le_bytes());
            dst.extend_from_slice(&ps.next.to_le_bytes());
            frame::seal(dst, s);
        }
        for (pid, ps) in &self.parts {
            for (name, (next, state)) in &ps.groups {
                let s = frame::begin(dst, frame::T_CP_GROUP);
                dst.extend_from_slice(&pid.to_le_bytes());
                dst.extend_from_slice(&(name.len() as u16).to_le_bytes());
                dst.extend_from_slice(name);
                dst.extend_from_slice(&next.to_le_bytes());
                dst.extend_from_slice(state);
                frame::seal(dst, s);
            }
        }
        let s = frame::begin(dst, frame::T_CP_END);
        dst.extend_from_slice(&id.to_le_bytes());
        counts(dst);
        frame::seal(dst, s);
    }
}

/// Collects a checkpoint's frames in order: config, topic definitions, partitions, groups, end.
struct CpBuilder {
    id: u64,
    segment: u64,
    want: (usize, usize, usize),
    got: (usize, usize, usize),
    saw_config: bool,
    st: LogState,
    last_group: Option<(u32, Vec<u8>)>,
    last_pid: Option<u32>,
    last_part: Option<u32>,
}

impl CpBuilder {
    fn begin(body: &[u8]) -> Result<Self, &'static str> {
        if body.len() != 28 {
            return Err("cp-begin size");
        }
        let u = |i: usize| u32::from_le_bytes(body[i..i + 4].try_into().unwrap()) as usize;
        Ok(CpBuilder {
            id: u64::from_le_bytes(body[..8].try_into().unwrap()),
            segment: u64::from_le_bytes(body[8..16].try_into().unwrap()),
            want: (u(16), u(20), u(24)),
            got: (0, 0, 0),
            saw_config: false,
            st: LogState::default(),
            last_group: None,
            last_pid: None,
            last_part: None,
        })
    }

    fn add(&mut self, typ: u8, body: &[u8]) -> Result<bool, &'static str> {
        if !self.saw_config {
            if typ != frame::T_CONFIG {
                return Err("expected the config frame");
            }
            self.st.config = body.to_vec();
            self.saw_config = true;
        } else if self.got.0 < self.want.0 {
            if typ != frame::T_TOPIC_DEF || body.len() < 8 {
                return Err("expected a topic definition");
            }
            let pid = u32::from_le_bytes(body[..4].try_into().unwrap());
            if self.last_pid.is_some_and(|p| pid <= p) {
                return Err("topic definitions out of order");
            }
            self.last_pid = Some(pid);
            self.st.define(pid, &body[8..], u32::from_le_bytes(body[4..8].try_into().unwrap()))?;
            self.got.0 += 1;
        } else if self.got.1 < self.want.1 {
            if typ != frame::T_CP_PARTITION || body.len() != 20 {
                return Err("expected a partition entry");
            }
            let pid = u32::from_le_bytes(body[..4].try_into().unwrap());
            if self.last_part.is_some_and(|p| pid <= p) {
                return Err("partition entries out of order");
            }
            self.last_part = Some(pid);
            let ps = self.st.parts.get_mut(&pid).ok_or("partition entry for an undefined id")?;
            ps.base = u64::from_le_bytes(body[4..12].try_into().unwrap());
            ps.next = u64::from_le_bytes(body[12..20].try_into().unwrap());
            if ps.base > ps.next {
                return Err("partition floor past its end");
            }
            self.got.1 += 1;
        } else if self.got.2 < self.want.2 {
            if typ != frame::T_CP_GROUP || body.len() < 14 {
                return Err("expected a group entry");
            }
            let pid = u32::from_le_bytes(body[..4].try_into().unwrap());
            let g = u16::from_le_bytes(body[4..6].try_into().unwrap()) as usize;
            if body.len() < 6 + g + 8 {
                return Err("group name exceeds the entry");
            }
            let name = body[6..6 + g].to_vec();
            let key = (pid, name.clone());
            if self.last_group.as_ref().is_some_and(|l| key <= *l) {
                return Err("group entries out of order");
            }
            self.last_group = Some(key);
            let next = u64::from_le_bytes(body[6 + g..14 + g].try_into().unwrap());
            let ps = self.st.parts.get_mut(&pid).ok_or("group for an undefined partition id")?;
            if next > ps.next {
                return Err("group past the end of its partition");
            }
            ps.groups.insert(name, (next, body[14 + g..].to_vec()));
            self.got.2 += 1;
        } else {
            if typ != frame::T_CP_END || body.len() != 20 {
                return Err("expected cp-end");
            }
            let u = |i: usize| u32::from_le_bytes(body[i..i + 4].try_into().unwrap()) as usize;
            if u64::from_le_bytes(body[..8].try_into().unwrap()) != self.id || (u(8), u(12), u(16)) != self.want || self.got.1 != self.st.by_pid.len() {
                return Err("cp-end does not match cp-begin");
            }
            return Ok(true);
        }
        Ok(false)
    }
}

// ----- per-segment sparse index -----

#[derive(Default)]
struct IdxPart {
    first: u64,
    count: u64,
    offs: Vec<u64>,
    poss: Vec<u32>,
}

#[derive(Default)]
struct SegIndex {
    start_next: HashMap<u32, u64>,
    parts: HashMap<u32, IdxPart>,
}

impl SegIndex {
    fn starting(st: &LogState) -> Self {
        SegIndex { start_next: st.parts.iter().map(|(p, s)| (*p, s.next)).collect(), parts: HashMap::new() }
    }

    fn observe(&mut self, pid: u32, offset: u64, pos: u64) {
        let p = self.parts.entry(pid).or_insert_with(|| IdxPart { first: offset, ..Default::default() });
        if p.count == 0 || offset % SPARSE_STRIDE == 0 {
            p.offs.push(offset);
            p.poss.push(pos as u32);
        }
        p.count += 1;
    }

    /// The partition's first offset in this segment (or the offset its next record will get).
    fn first(&self, pid: u32) -> u64 {
        match self.parts.get(&pid) {
            Some(p) => p.first,
            None => self.start_next.get(&pid).copied().unwrap_or(0),
        }
    }

    /// A position at or before the record.
    fn find(&self, pid: u32, off: u64) -> Option<u64> {
        let p = self.parts.get(&pid)?;
        if off < p.first || off >= p.first + p.count {
            return None;
        }
        let i = p.offs.partition_point(|&o| o <= off);
        Some(p.poss[i - 1] as u64)
    }
}

// ----- reading frames from a file through a window -----

struct FrameReader<'a> {
    f: &'a File,
    size: u64,
    buf: Vec<u8>,
    at: u64,
    chunk: usize,
}

impl<'a> FrameReader<'a> {
    fn new(f: &'a File, size: u64, chunk: usize) -> Self {
        FrameReader { f, size, buf: Vec::new(), at: 0, chunk }
    }

    fn fill(&mut self, off: u64, want: usize) -> io::Result<()> {
        let want = want.min((self.size - off) as usize);
        self.buf.resize(want, 0);
        self.at = off;
        self.f.read_exact_at(&mut self.buf, off)
    }

    /// The frame at `off`: (type, range of the body within self.buf, total size).
    fn next(&mut self, off: u64) -> io::Result<Result<(u8, std::ops::Range<usize>, usize), FrameError>> {
        if off < self.at || off >= self.at + self.buf.len() as u64 {
            self.fill(off, self.chunk)?;
        }
        let rel = (off - self.at) as usize;
        let mut res = frame::read(&self.buf[rel..]).map(|(t, b, n)| (t, b.len(), n));
        if res == Err(FrameError::Short) && self.at + (self.buf.len() as u64) < self.size {
            let have = self.buf.len() - rel;
            let mut want = frame::HEADER;
            if have >= frame::HEADER {
                want += u32::from_le_bytes(self.buf[rel..rel + 4].try_into().unwrap()) as usize;
            } else {
                self.fill(off, frame::HEADER)?;
                match frame::read(&self.buf) {
                    Err(FrameError::Short) if self.buf.len() >= frame::HEADER => {
                        want += u32::from_le_bytes(self.buf[..4].try_into().unwrap()) as usize;
                    }
                    other => return Ok(other.map(|(t, b, n)| (t, frame::HEADER..frame::HEADER + b.len(), n))),
                }
            }
            self.fill(off, self.chunk.max(want.min(frame::HEADER + frame::MAX_BODY)))?;
            res = frame::read(&self.buf).map(|(t, b, n)| (t, b.len(), n));
        }
        let rel = (off - self.at) as usize;
        Ok(res.map(|(t, blen, n)| (t, rel + frame::HEADER..rel + frame::HEADER + blen, n)))
    }
}

struct Scan {
    state: LogState,
    index: SegIndex,
    data_start: u64,
    end: u64,
    torn_bytes: u64,
}

/// Validates a segment's header and start checkpoint and replays every frame after it.
/// With `may_truncate` (the active segment) a torn tail is cut off and the file synced; a sealed
/// segment with a torn tail is corrupt.
fn scan_segment(dir: &Path, id: u64, may_truncate: bool) -> io::Result<(File, Scan)> {
    let f = OpenOptions::new().read(true).write(may_truncate).open(dir.join(seg_name(id)))?;
    let size = f.metadata()?.len();
    if size < SEG_HEADER as u64 {
        return Err(corrupt(id, 0, "shorter than the header"));
    }
    let mut hdr = [0u8; SEG_HEADER];
    f.read_exact_at(&mut hdr, 0)?;
    check_seg_header(&hdr).map_err(|e| corrupt(id, 0, e))?;
    let mut st = LogState::default();
    let mut index = SegIndex::default();
    let mut cp: Option<CpBuilder> = None;
    let (mut at, mut cp_start, mut torn) = (SEG_HEADER as u64, 0u64, false);
    let (mut checkpoints, mut data_start) = (0u32, 0u64);
    let mut fr = FrameReader::new(&f, size, 1 << 20);
    while at < size {
        let (typ, body, n) = match fr.next(at)? {
            Ok(x) => x,
            Err(FrameError::Short) => {
                torn = true;
                break;
            }
            Err(e) => return Err(corrupt(id, at, format!("{e:?}"))),
        };
        let body = &fr.buf[body];
        if at == SEG_HEADER as u64 && typ != frame::T_CP_BEGIN {
            return Err(corrupt(id, at, "the first frame is not the start checkpoint"));
        }
        let bad = |e: &'static str| corrupt(id, at, e);
        if let Some(b) = cp.as_mut() {
            if b.add(typ, body).map_err(bad)? {
                let b = cp.take().unwrap();
                if b.segment != id {
                    return Err(corrupt(id, at, "checkpoint names another segment"));
                }
                if checkpoints > 0 {
                    // A later (retention) checkpoint must agree with the frames before it.
                    for (pid, ps) in &st.parts {
                        if b.st.parts.get(pid).map(|p| p.next) != Some(ps.next) {
                            return Err(corrupt(id, at, "a checkpoint disagrees with the frames before it"));
                        }
                    }
                }
                st = b.st;
                checkpoints += 1;
                if checkpoints == 1 {
                    data_start = at + n as u64;
                    index = SegIndex::starting(&st);
                }
            }
        } else {
            match typ {
                frame::T_CP_BEGIN => {
                    cp_start = at;
                    cp = Some(CpBuilder::begin(body).map_err(bad)?);
                }
                frame::T_RECORD => {
                    let r = frame::parse_record(body).map_err(|e| corrupt(id, at, format!("{e:?}")))?;
                    st.apply_record(r.partition, r.offset).map_err(bad)?;
                    index.observe(r.partition, r.offset, at);
                }
                frame::T_COMMIT => {
                    let c = frame::parse_commit(body).map_err(|e| corrupt(id, at, format!("{e:?}")))?;
                    st.apply_commit(&c).map_err(bad)?;
                }
                frame::T_TOPIC_DEF => {
                    if body.len() < 8 {
                        return Err(bad("topic definition size"));
                    }
                    st.define(u32::from_le_bytes(body[..4].try_into().unwrap()), &body[8..], u32::from_le_bytes(body[4..8].try_into().unwrap())).map_err(bad)?;
                }
                frame::T_FENCE if body.is_empty() => {}
                _ => return Err(bad("unexpected frame type")),
            }
        }
        at += n as u64;
    }
    if cp.is_some() {
        if checkpoints == 0 {
            return Err(corrupt(id, cp_start, "the start checkpoint is incomplete"));
        }
        at = cp_start; // an incomplete retention checkpoint
        torn = true;
    }
    if checkpoints == 0 {
        return Err(corrupt(id, SEG_HEADER as u64, "no start checkpoint"));
    }
    if torn || at < size {
        if !may_truncate {
            return Err(corrupt(id, at, "a sealed segment has a torn or partial tail"));
        }
        f.set_len(at)?;
    }
    if may_truncate {
        // Even a complete frame may have survived a process crash only in the page cache.
        f.sync_all()?;
    }
    Ok((f, Scan { state: st, index, data_start, end: at, torn_bytes: size - at }))
}

fn create_segment(dir: &Path, id: u64, st: &LogState) -> io::Result<(File, u64)> {
    let mut data = seg_header();
    st.checkpoint(id, id, &mut data);
    let tmp = dir.join(format!("seg-{id:016x}.tmp"));
    let f = OpenOptions::new().read(true).write(true).create_new(true).mode(0o600).open(&tmp)?;
    let staged = (|| {
        f.write_all_at(&data, 0)?;
        f.sync_all()?;
        fs::rename(&tmp, dir.join(seg_name(id)))
    })();
    if let Err(e) = staged {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    sync_dir(dir)?;
    Ok((f, data.len() as u64))
}

fn write_range(dir: &Path, first: u64, last: u64) -> io::Result<()> {
    let tmp = dir.join("RANGE.tmp");
    let f = OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&tmp)?;
    f.write_all_at(&range_bytes(first, last), 0)?;
    f.sync_all()?;
    drop(f);
    fs::rename(&tmp, dir.join("RANGE"))?;
    sync_dir(dir)
}

// ----- the store -----

#[derive(Clone)]
pub struct Options {
    /// Roll the active segment when its appended tail reaches this many bytes.
    pub segment_target: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options { segment_target: SEGMENT_TARGET }
    }
}

struct Inner {
    state: LogState,
    active: File,
    active_id: u64,
    data_start: u64,
    end: u64,
    idx: SegIndex,
    sealed: Vec<u64>,
    cache: HashMap<u64, Arc<SegIndex>>,
    cache_order: VecDeque<u64>,
    buf: Vec<u8>,
    failed: bool,
    torn_bytes: u64,
}

struct Slot {
    result: Mutex<Option<io::Result<(u64, i64)>>>,
    ready: Condvar,
}

struct Queued {
    topic: Vec<u8>,
    key: Vec<u8>,
    value: Vec<u8>,
    slot: Arc<Slot>,
}

#[derive(Default)]
struct Queue {
    active: bool,
    pending: Vec<Queued>,
    bytes: usize,
    wake: bool,
    stop: bool,
}

struct Shared {
    seg_dir: PathBuf,
    opts: Options,
    inner: Mutex<Inner>,
    queue: Mutex<Queue>,
    drain: Condvar,
    groups: AtomicU64,
    calls: AtomicU64,
    closed: AtomicBool,
}

#[derive(Debug, Clone, Copy)]
pub struct PublishStats {
    /// Durable writes attempted for publishes (each ends in one sync) and the calls in them.
    pub groups: u64,
    pub calls: u64,
}

pub struct Store {
    shared: Arc<Shared>,
    drainer: Option<std::thread::JoinHandle<()>>,
    _lock: File,
    _journal: File,
}

fn now_nanos() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as i64).unwrap_or(0)
}

impl Inner {
    /// Appends records for (topic, key, value) items with one write and one sync.
    /// Returns the offset and time given to each item.
    fn publish(&mut self, sh: &Shared, items: &[(&[u8], &[u8], &[u8])], out: &mut Vec<(u64, i64)>) -> io::Result<()> {
        if self.failed {
            return Err(Error::other("the log fenced itself after a failed write; reopen it"));
        }
        let mut buf = std::mem::take(&mut self.buf);
        buf.clear();
        out.clear();
        let mut steps: Vec<(u32, u64, usize)> = Vec::with_capacity(items.len());
        let mut defined: Vec<(u32, Vec<u8>)> = Vec::new();
        let mut counts: HashMap<u32, u64> = HashMap::new();
        let mut next_pid = self.state.next_pid;
        for (topic, key, value) in items {
            if value.len() > 1 << 20 || key.len() > 4096 {
                return Err(invalid("message too large"));
            }
            let part = (topic.to_vec(), 0u32);
            let pid = match self.state.by_part.get(&part) {
                Some(p) => *p,
                None => match defined.iter().find(|d| d.1 == *topic) {
                    Some(d) => d.0,
                    None => {
                        let pid = next_pid;
                        next_pid += 1;
                        frame::append_topic_def(&mut buf, pid, 0, topic);
                        defined.push((pid, topic.to_vec()));
                        pid
                    }
                },
            };
            let base = self.state.parts.get(&pid).map_or(0, |p| p.next);
            let c = counts.entry(pid).or_insert(0);
            let offset = base + *c;
            *c += 1;
            let time = now_nanos();
            steps.push((pid, offset, buf.len()));
            frame::append_record(&mut buf, &frame::Record { partition: pid, offset, time, key, id: b"", value });
            out.push((offset, time));
            if buf.len() > frame::MAX_BODY {
                return Err(invalid("batch too large"));
            }
        }
        let res = self.append(sh, &buf);
        if res.is_ok() {
            for (pid, topic) in &defined {
                self.state.define(*pid, topic, 0).map_err(Error::other)?;
            }
            let start = self.end - buf.len() as u64;
            for (pid, offset, at) in steps {
                self.state.parts.get_mut(&pid).unwrap().next += 1;
                self.idx.observe(pid, offset, start + at as u64);
            }
        }
        if buf.capacity() <= 64 << 10 {
            self.buf = buf;
        }
        res
    }

    fn append(&mut self, sh: &Shared, frames: &[u8]) -> io::Result<()> {
        if self.end - self.data_start >= sh.opts.segment_target {
            self.roll(sh)?;
        }
        if frames.len() as u64 > u32::MAX as u64 - self.end {
            return Err(invalid("the segment has no indexable room"));
        }
        let res = self.active.write_all_at(frames, self.end).and_then(|_| self.active.sync_data());
        if res.is_err() {
            self.failed = true;
            return res;
        }
        self.end += frames.len() as u64;
        Ok(())
    }

    /// Publishes the next segment (stage, sync, rename, sync the directory), then the range file.
    fn roll(&mut self, sh: &Shared) -> io::Result<()> {
        let id = self.active_id + 1;
        let made = create_segment(&sh.seg_dir, id, &self.state).and_then(|(f, end)| {
            let first = self.sealed.first().copied().unwrap_or(self.active_id);
            write_range(&sh.seg_dir, first, id).map(|_| (f, end))
        });
        let (f, end) = match made {
            Ok(x) => x,
            Err(e) => {
                self.failed = true;
                return Err(e);
            }
        };
        let old = std::mem::replace(&mut self.idx, SegIndex::starting(&self.state));
        self.cache_put(self.active_id, Arc::new(old));
        self.sealed.push(self.active_id);
        self.active = f;
        self.active_id = id;
        self.data_start = end;
        self.end = end;
        Ok(())
    }

    fn cache_put(&mut self, id: u64, x: Arc<SegIndex>) {
        if self.cache.insert(id, x).is_none() {
            self.cache_order.push_back(id);
        }
        while self.cache_order.len() > INDEX_CACHE {
            let gone = self.cache_order.pop_front().unwrap();
            self.cache.remove(&gone);
        }
    }

    fn sealed_index(&mut self, sh: &Shared, id: u64) -> io::Result<Arc<SegIndex>> {
        if let Some(x) = self.cache.get(&id) {
            return Ok(x.clone());
        }
        let (_, scan) = scan_segment(&sh.seg_dir, id, false)?;
        let x = Arc::new(scan.index);
        self.cache_put(id, x.clone());
        Ok(x)
    }

    fn locate(&mut self, sh: &Shared, pid: u32, off: u64) -> io::Result<Option<(u64, u64)>> {
        if off >= self.idx.first(pid) {
            return Ok(self.idx.find(pid, off).map(|p| (self.active_id, p)));
        }
        let ids = self.sealed.clone();
        let (mut lo, mut hi) = (0usize, ids.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.sealed_index(sh, ids[mid])?.first(pid) > off {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if lo == 0 {
            return Ok(None);
        }
        let id = ids[lo - 1];
        Ok(self.sealed_index(sh, id)?.find(pid, off).map(|p| (id, p)))
    }
}

impl Store {
    /// Opens the store in `dir`, creating it with `config` (the manifest JSON the Go engine
    /// writes for its Config) when the directory holds none.
    pub fn open(dir: &Path, config: &[u8], opts: Options) -> io::Result<Store> {
        fs::create_dir_all(dir)?;
        let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false).mode(0o600).open(dir.join("journal.lock"))?;
        lock.try_lock().map_err(|_| Error::other("lock data directory: held by another process"))?;
        let journal = OpenOptions::new().read(true).write(true).create(true).truncate(false).mode(0o600).open(dir.join("journal.v1"))?;
        journal.try_lock().map_err(|_| Error::other("lock data directory: held by another process"))?;
        sync_dir(dir)?;
        let mut head = vec![0u8; MARKER.len()];
        let got = journal.read_at(&mut head, 0)?;
        let seg_dir = dir.join("segments");
        let marked = &head[..got] == MARKER;
        if !marked && got != 0 {
            return Err(invalid("not a segmented store (legacy journal, workflow store or damaged marker)"));
        }
        let inner = if marked {
            let mut range = Vec::new();
            {
                use std::io::Read;
                File::open(seg_dir.join("RANGE")).and_then(|mut f| f.read_to_end(&mut range)).map_err(|e| corrupt(0, 0, format!("segment range: {e}")))?;
            }
            if range.len() != 20 || range_bytes(u64::from_le_bytes(range[..8].try_into().unwrap()), u64::from_le_bytes(range[8..16].try_into().unwrap()))[..] != range[..] {
                return Err(corrupt(0, 0, "invalid segment range"));
            }
            let mut ids = Vec::new();
            for e in fs::read_dir(&seg_dir)? {
                let e = e?;
                let name = e.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("seg-") && name.ends_with(".tmp") {
                    fs::remove_file(e.path())?; // staged and never published
                } else if let Some(id) = parse_seg_name(&name) {
                    ids.push(id);
                }
            }
            ids.sort_unstable();
            if ids.windows(2).any(|w| w[1] != w[0] + 1) {
                return Err(corrupt(0, 0, "a segment is missing"));
            }
            let top = *ids.last().ok_or_else(|| Error::other("segmented store has no active segment"))?;
            let (f, scan) = scan_segment(&seg_dir, top, true)?;
            ids.pop();
            Inner { state: scan.state, active: f, active_id: top, data_start: scan.data_start, end: scan.end, idx: scan.index, sealed: ids,
                    cache: HashMap::new(), cache_order: VecDeque::new(), buf: Vec::new(), failed: false, torn_bytes: scan.torn_bytes }
        } else {
            if seg_dir.exists() {
                return Err(Error::other("unmarked segmented directory; refusing initialization"));
            }
            fs::DirBuilder::new().mode(0o700).create(&seg_dir)?;
            let state = LogState { config: config.to_vec(), ..Default::default() };
            let (f, end) = create_segment(&seg_dir, 1, &state)?;
            write_range(&seg_dir, 1, 1)?;
            sync_dir(dir)?;
            journal.write_all_at(MARKER, 0)?;
            journal.sync_all()?;
            Inner { idx: SegIndex::starting(&state), state, active: f, active_id: 1, data_start: end, end, sealed: Vec::new(),
                    cache: HashMap::new(), cache_order: VecDeque::new(), buf: Vec::new(), failed: false, torn_bytes: 0 }
        };
        let shared = Arc::new(Shared { seg_dir, opts, inner: Mutex::new(inner), queue: Mutex::new(Queue::default()), drain: Condvar::new(),
                                       groups: AtomicU64::new(0), calls: AtomicU64::new(0), closed: AtomicBool::new(false) });
        let worker = shared.clone();
        let drainer = std::thread::Builder::new().name("publish-drain".into()).spawn(move || drain_loop(&worker))?;
        Ok(Store { shared, drainer: Some(drainer), _lock: lock, _journal: journal })
    }

    /// Bytes a torn tail lost at open (zero when the log ended cleanly).
    pub fn truncated_bytes(&self) -> u64 {
        self.shared.inner.lock().unwrap().torn_bytes
    }

    pub fn stats(&self) -> PublishStats {
        PublishStats { groups: self.shared.groups.load(Ordering::Relaxed), calls: self.shared.calls.load(Ordering::Relaxed) }
    }

    /// The next offset of a single-partition topic (0 when it has no records).
    pub fn next_offset(&self, topic: &str) -> u64 {
        let inner = self.shared.inner.lock().unwrap();
        inner.state.by_part.get(&(topic.as_bytes().to_vec(), 0)).and_then(|p| inner.state.parts.get(p)).map_or(0, |p| p.next)
    }

    pub fn segments(&self) -> (usize, u64) {
        let inner = self.shared.inner.lock().unwrap();
        (inner.sealed.len(), inner.active_id)
    }

    /// Publishes one record and returns its offset and time once it is durable.
    ///
    /// As in the Go engine: an isolated caller writes inline; callers that arrive during a write
    /// queue up and are written as one group with one sync.
    pub fn publish(&self, topic: &str, key: &[u8], value: &[u8]) -> io::Result<(u64, i64)> {
        let sh = &*self.shared;
        let mut q = sh.queue.lock().unwrap();
        let item = [(topic.as_bytes(), key, value)];
        let mut out = Vec::with_capacity(1);
        if !q.active {
            if let Ok(mut inner) = sh.inner.try_lock() {
                drop(q);
                sh.note(1);
                inner.publish(sh, &item, &mut out)?;
                return Ok(out[0]);
            }
            q.active = true;
            drop(q);
            let res = {
                let mut inner = sh.inner.lock().unwrap();
                sh.note(1);
                inner.publish(sh, &item, &mut out)
            };
            // Hand over to the drainer: it takes whatever queued meanwhile, or clears `active`.
            let mut q = sh.queue.lock().unwrap();
            q.wake = true;
            sh.drain.notify_one();
            drop(q);
            return res.map(|_| out[0]);
        }
        let size = topic.len() + key.len() + value.len();
        if q.pending.len() >= MAX_QUEUED || size > MAX_QUEUED_BYTES - q.bytes.min(MAX_QUEUED_BYTES) {
            drop(q);
            let mut inner = sh.inner.lock().unwrap();
            sh.note(1);
            inner.publish(sh, &item, &mut out)?;
            return Ok(out[0]);
        }
        let slot = Arc::new(Slot { result: Mutex::new(None), ready: Condvar::new() });
        q.pending.push(Queued { topic: topic.as_bytes().to_vec(), key: key.to_vec(), value: value.to_vec(), slot: slot.clone() });
        q.bytes += size;
        drop(q);
        let mut r = slot.result.lock().unwrap();
        loop {
            if let Some(res) = r.take() {
                return res;
            }
            r = slot.ready.wait(r).unwrap();
        }
    }

    /// Publishes several records to one topic with one write and one sync.
    pub fn publish_batch(&self, topic: &str, values: &[&[u8]]) -> io::Result<Vec<(u64, i64)>> {
        let sh = &*self.shared;
        let items: Vec<(&[u8], &[u8], &[u8])> = values.iter().map(|v| (topic.as_bytes(), &b""[..], *v)).collect();
        let mut out = Vec::with_capacity(items.len());
        let mut inner = sh.inner.lock().unwrap();
        sh.note(items.len() as u64);
        inner.publish(sh, &items, &mut out)?;
        Ok(out)
    }

    /// Reads up to `max` records of a single-partition topic from `start`, calling `visit`
    /// with (offset, time, key, value). Returns the next offset to read.
    pub fn read(&self, topic: &str, start: u64, max: usize, mut visit: impl FnMut(u64, i64, &[u8], &[u8])) -> io::Result<u64> {
        let sh = &*self.shared;
        let mut inner = sh.inner.lock().unwrap();
        let Some(&pid) = inner.state.by_part.get(&(topic.as_bytes().to_vec(), 0)) else { return Ok(start) };
        let end = inner.state.parts[&pid].next;
        let (mut next, mut taken) = (start, 0usize);
        while next < end && taken < max {
            let Some((id, mut pos)) = inner.locate(sh, pid, next)? else {
                return Err(Error::new(ErrorKind::NotFound, format!("no record at offset {next}")));
            };
            let before = next;
            let opened;
            let (f, size) = if id == inner.active_id {
                (&inner.active, inner.end)
            } else {
                opened = File::open(sh.seg_dir.join(seg_name(id)))?;
                let size = opened.metadata()?.len();
                (&opened, size)
            };
            let mut fr = FrameReader::new(f, size, 64 << 10);
            while pos < size && taken < max {
                let (typ, body, n) = fr.next(pos)?.map_err(|e| corrupt(id, pos, format!("{e:?}")))?;
                pos += n as u64;
                if typ != frame::T_RECORD {
                    continue;
                }
                let r = frame::parse_record(&fr.buf[body]).map_err(|e| corrupt(id, pos, format!("{e:?}")))?;
                if r.partition != pid || r.offset < next {
                    continue;
                }
                if r.offset != next {
                    return Err(corrupt(id, pos, "missing offset"));
                }
                visit(r.offset, r.time, r.key, r.value);
                next += 1;
                taken += 1;
            }
            if next == before {
                return Err(corrupt(id, pos, "the index points past the record"));
            }
        }
        Ok(next)
    }
}

impl Shared {
    fn note(&self, calls: u64) {
        self.groups.fetch_add(1, Ordering::Relaxed);
        self.calls.fetch_add(calls, Ordering::Relaxed);
    }
}

fn drain_loop(sh: &Shared) {
    loop {
        let mut q = sh.queue.lock().unwrap();
        while !q.wake && !q.stop {
            q = sh.drain.wait(q).unwrap();
        }
        if q.stop {
            return;
        }
        q.wake = false;
        loop {
            let batch = std::mem::take(&mut q.pending);
            q.bytes = 0;
            if batch.is_empty() {
                q.active = false;
                break;
            }
            drop(q);
            let items: Vec<(&[u8], &[u8], &[u8])> = batch.iter().map(|w| (&w.topic[..], &w.key[..], &w.value[..])).collect();
            let mut out = Vec::with_capacity(items.len());
            let res = {
                let mut inner = sh.inner.lock().unwrap();
                sh.note(items.len() as u64);
                inner.publish(sh, &items, &mut out)
            };
            let released = batch.len();
            for (i, w) in batch.iter().enumerate() {
                let r = match &res {
                    Ok(()) => Ok(out[i]),
                    Err(e) => Err(Error::new(e.kind(), e.to_string())),
                };
                *w.slot.result.lock().unwrap() = Some(r);
                w.slot.ready.notify_one();
            }
            drop(batch);
            // Give the callers just released a moment to come back, so groups stay full.
            if released >= 2 {
                let deadline = Instant::now() + GATHER_LIMIT;
                loop {
                    let pending = sh.queue.lock().unwrap().pending.len();
                    if pending >= released || Instant::now() >= deadline {
                        break;
                    }
                    std::thread::yield_now();
                }
            }
            q = sh.queue.lock().unwrap();
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Relaxed);
        {
            let mut q = self.shared.queue.lock().unwrap();
            q.stop = true;
            self.shared.drain.notify_one();
        }
        if let Some(h) = self.drainer.take() {
            let _ = h.join();
        }
    }
}
