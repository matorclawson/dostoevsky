//! CRC-32C (Castagnoli), as the Go engine's crc32.MakeTable(crc32.Castagnoli).
//! Uses the CPU's CRC instructions when present, else a slicing-by-8 table.

const POLY: u32 = 0x82F63B78;

const fn make_tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
            k += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    let mut i = 0;
    while i < 256 {
        let mut k = 1;
        while k < 8 {
            let p = t[k - 1][i];
            t[k][i] = (p >> 8) ^ t[0][(p & 0xFF) as usize];
            k += 1;
        }
        i += 1;
    }
    t
}

static TABLES: [[u32; 256]; 8] = make_tables();

fn update_table(mut crc: u32, mut b: &[u8]) -> u32 {
    while b.len() >= 8 {
        let lo = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) ^ crc;
        let hi = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
        crc = TABLES[7][(lo & 0xFF) as usize]
            ^ TABLES[6][((lo >> 8) & 0xFF) as usize]
            ^ TABLES[5][((lo >> 16) & 0xFF) as usize]
            ^ TABLES[4][(lo >> 24) as usize]
            ^ TABLES[3][(hi & 0xFF) as usize]
            ^ TABLES[2][((hi >> 8) & 0xFF) as usize]
            ^ TABLES[1][((hi >> 16) & 0xFF) as usize]
            ^ TABLES[0][(hi >> 24) as usize];
        b = &b[8..];
    }
    for &x in b {
        crc = TABLES[0][((crc ^ x as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn update_hw(crc: u32, b: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};
    let mut c = crc as u64;
    let mut chunks = b.chunks_exact(8);
    for ch in &mut chunks {
        c = _mm_crc32_u64(c, u64::from_le_bytes(ch.try_into().unwrap()));
    }
    let mut c = c as u32;
    for &x in chunks.remainder() {
        c = _mm_crc32_u8(c, x);
    }
    c
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn update_hw(crc: u32, b: &[u8]) -> u32 {
    use std::arch::aarch64::{__crc32cb, __crc32cd};
    let mut c = crc;
    let mut chunks = b.chunks_exact(8);
    for ch in &mut chunks {
        c = __crc32cd(c, u64::from_le_bytes(ch.try_into().unwrap()));
    }
    for &x in chunks.remainder() {
        c = __crc32cb(c, x);
    }
    c
}

/// Continues a checksum, as Go's crc32.Update.
pub fn update(crc: u32, b: &[u8]) -> u32 {
    let c = !crc;
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("sse4.2") {
        return !unsafe { update_hw(c, b) };
    }
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("crc") {
        return !unsafe { update_hw(c, b) };
    }
    !update_table(c, b)
}

pub fn checksum(b: &[u8]) -> u32 {
    update(0, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn known_values() {
        assert_eq!(checksum(b"123456789"), 0xE3069283);
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 7 + 3) as u8).collect();
        assert_eq!(!update_table(!0, &data), checksum(&data));
        assert_eq!(update(checksum(&data[..333]), &data[333..]), checksum(&data));
    }
}
