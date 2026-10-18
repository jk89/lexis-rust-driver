//! Order-preserving encoding of partition keys, as used by Lexis'
//! OrderedPartitioner (`dht/ordered_encoding.cc` in Lexis). The token of a
//! partition is this encoding, so it must match the server byte for byte.
//!
//! Each component is type-encoded, its 0x00 bytes are escaped as 0x00 0x01,
//! and it is terminated by 0x00 0x00. Unsigned byte order of the result is
//! partition key order for the types with an exact rule. A component of a
//! column declared DESC in `PARTITION ORDER BY` is that encoding bitwise
//! complemented, terminator included. The design (`Direction`, the 0xFF 0xFF
//! descending terminator) is from kaebase:
//! https://github.com/kaepek/kaebase/blob/FEAT/scaling/v2/crates/common/src/encoding/cluster_key.rs

use scylla_cql::frame::response::result::{ColumnType, NativeType};

fn escape_byte(buf: &mut Vec<u8>, b: u8) {
    if b == 0 {
        buf.extend_from_slice(&[0x00, 0x01]);
    } else {
        buf.push(b);
    }
}

fn escape_all(buf: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        escape_byte(buf, b);
    }
}

fn terminate(buf: &mut Vec<u8>) {
    buf.extend_from_slice(&[0x00, 0x00]);
}

// Float lex transform ported from kaebase v2:
// https://github.com/kaepek/kaebase/blob/FEAT/scaling/v2/crates/common/src/encoding/float.rs
fn float64_to_lex_u64(v: f64) -> u64 {
    // CQL compares floats as Java does: every NaN equals every other NaN and
    // sorts above everything, so all NaNs encode as the canonical quiet NaN.
    let v = if v.is_nan() { f64::NAN } else { v };
    let bits = v.to_bits();
    if bits >> 63 == 0 {
        bits | (1u64 << 63)
    } else {
        !bits
    }
}

/// Reads up to 8 big-endian bytes as a non-negative/two's-complement-free
/// integer, the way the server reads timestamp/date/time values.
fn read_be_up_to_8(raw: &[u8]) -> u64 {
    raw.iter()
        .take(8)
        .fold(0u64, |acc, &b| (acc << 8) | b as u64)
}

fn encode_varint(buf: &mut Vec<u8>, twos_complement: &[u8]) {
    let mut mag = twos_complement.to_vec();
    let negative = mag.first().is_some_and(|b| b & 0x80 != 0);
    if negative {
        for b in mag.iter_mut() {
            *b = !*b;
        }
        for b in mag.iter_mut().rev() {
            *b = b.wrapping_add(1);
            if *b != 0 {
                break;
            }
        }
    }
    let start = mag.iter().position(|&b| b != 0).unwrap_or(mag.len());
    let len = (mag.len() - start) as u32;
    let flip = if negative { 0xff } else { 0x00 };
    escape_byte(buf, if negative { 0x7f } else { 0x80 });
    escape_all(
        buf,
        &(len ^ if negative { u32::MAX } else { 0 }).to_be_bytes(),
    );
    for &b in &mag[start..] {
        escape_byte(buf, b ^ flip);
    }
}

/// Converts a big-endian magnitude to its decimal digits.
fn magnitude_to_decimal(mut mag: Vec<u8>) -> String {
    let mut digits = Vec::new();
    while mag.iter().any(|&b| b != 0) {
        let mut rem = 0u32;
        for b in mag.iter_mut() {
            let cur = (rem << 8) | *b as u32;
            *b = (cur / 10) as u8;
            rem = cur % 10;
        }
        digits.push(b'0' + rem as u8);
    }
    if digits.is_empty() {
        digits.push(b'0');
    }
    digits.reverse();
    String::from_utf8(digits).unwrap()
}

fn encode_decimal(buf: &mut Vec<u8>, raw: &[u8]) -> Option<()> {
    if raw.len() < 4 {
        return None;
    }
    let mut scale = i32::from_be_bytes(raw[..4].try_into().ok()?) as i64;
    let unscaled = &raw[4..];
    let negative = unscaled.first().is_some_and(|b| b & 0x80 != 0);
    let mut mag = unscaled.to_vec();
    if negative {
        for b in mag.iter_mut() {
            *b = !*b;
        }
        for b in mag.iter_mut().rev() {
            *b = b.wrapping_add(1);
            if *b != 0 {
                break;
            }
        }
    }
    if mag.iter().all(|&b| b == 0) {
        escape_byte(buf, 0x80);
        return Some(());
    }
    let mut digits = magnitude_to_decimal(mag);
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
        scale -= 1;
    }
    let exponent = digits.len() as i64 - scale;
    let biased = (exponent as u64) ^ (1u64 << 63);
    let flip = if negative { 0xff } else { 0x00 };
    escape_byte(buf, if negative { 0x7f } else { 0x81 });
    escape_all(
        buf,
        &(biased ^ if negative { u64::MAX } else { 0 }).to_be_bytes(),
    );
    for b in digits.bytes() {
        escape_byte(buf, b ^ flip);
    }
    if negative {
        escape_byte(buf, 0xff);
    }
    Some(())
}

// time_hi (version masked), time_mid, time_low.
fn time_fields(raw: &[u8]) -> [u8; 8] {
    [
        raw[6] & 0x0f,
        raw[7],
        raw[4],
        raw[5],
        raw[0],
        raw[1],
        raw[2],
        raw[3],
    ]
}

// CQL order: the time fields, the clock sequence and node as signed bytes,
// then the version nibble.
fn encode_timeuuid(buf: &mut Vec<u8>, raw: &[u8]) {
    escape_all(buf, &time_fields(raw));
    for &b in &raw[8..] {
        escape_byte(buf, b ^ 0x80);
    }
    escape_byte(buf, raw[6] >> 4);
}

// CQL order: the version, then for version 1 the time fields and the rest
// unsigned, for other versions all bytes in order.
fn encode_uuid(buf: &mut Vec<u8>, raw: &[u8]) {
    let version = raw[6] >> 4;
    escape_byte(buf, version);
    if version == 1 {
        escape_all(buf, &time_fields(raw));
        escape_all(buf, &raw[8..]);
    } else {
        escape_all(buf, raw);
    }
}

fn encode_component(buf: &mut Vec<u8>, typ: &ColumnType<'_>, raw: &[u8]) {
    if raw.is_empty() {
        terminate(buf);
        return;
    }
    match typ {
        ColumnType::Native(native) => match native {
            NativeType::BigInt | NativeType::Counter if raw.len() == 8 => {
                let v = u64::from_be_bytes(raw.try_into().unwrap()) ^ (1u64 << 63);
                escape_all(buf, &v.to_be_bytes());
            }
            NativeType::Int if raw.len() == 4 => {
                let v = u32::from_be_bytes(raw.try_into().unwrap()) ^ (1u32 << 31);
                escape_all(buf, &v.to_be_bytes());
            }
            NativeType::SmallInt if raw.len() == 2 => {
                let v = u16::from_be_bytes(raw.try_into().unwrap()) ^ (1u16 << 15);
                escape_all(buf, &v.to_be_bytes());
            }
            NativeType::TinyInt if raw.len() == 1 => {
                escape_byte(buf, raw[0] ^ 0x80);
            }
            NativeType::Boolean if raw.len() == 1 => {
                escape_byte(buf, if raw[0] != 0 { 1 } else { 0 });
            }
            NativeType::Timestamp | NativeType::Date | NativeType::Time => {
                let v = read_be_up_to_8(raw).wrapping_add(1u64 << 63);
                escape_all(buf, &v.to_be_bytes());
            }
            NativeType::Double if raw.len() == 8 => {
                let v = f64::from_be_bytes(raw.try_into().unwrap());
                escape_all(buf, &float64_to_lex_u64(v).to_be_bytes());
            }
            NativeType::Float if raw.len() == 4 => {
                let v = f32::from_be_bytes(raw.try_into().unwrap()) as f64;
                escape_all(buf, &float64_to_lex_u64(v).to_be_bytes());
            }
            NativeType::Varint => encode_varint(buf, raw),
            NativeType::Timeuuid if raw.len() == 16 => encode_timeuuid(buf, raw),
            NativeType::Uuid if raw.len() == 16 => encode_uuid(buf, raw),
            NativeType::Decimal => {
                if encode_decimal(buf, raw).is_none() {
                    escape_all(buf, raw);
                }
            }
            // Text, ascii, blob, inet, duration, and any
            // malformed fixed-size value: raw bytes, like the server.
            _ => escape_all(buf, raw),
        },
        // No order-preserving rule (tuples, UDTs, collections): serialized bytes.
        _ => escape_all(buf, raw),
    }
    terminate(buf);
}

/// Encodes partition key components (in partition key order) into the bytes
/// of an OrderedPartitioner token. `descending[i]` is whether component `i`
/// is DESC; missing entries are ASC.
pub(crate) fn encode_partition_key<'a, 'b: 'a>(
    components: impl Iterator<Item = (&'a ColumnType<'b>, &'a [u8])>,
    descending: &[bool],
) -> Vec<u8> {
    let mut buf = Vec::new();
    for (i, (typ, raw)) in components.enumerate() {
        let mark = buf.len();
        encode_component(&mut buf, typ, raw);
        if descending.get(i).copied().unwrap_or(false) {
            for b in &mut buf[mark..] {
                *b = !*b;
            }
        }
    }
    buf
}

/// The smallest byte string greater than every string that starts with `p`
/// (trailing 0xff bytes dropped, then the last byte incremented), or None if
/// `p` is all 0xff. The server's `dht::ordered_encoding::prefix_successor()`.
pub(crate) fn prefix_successor(mut p: Vec<u8>) -> Option<Vec<u8>> {
    while p.last() == Some(&0xff) {
        p.pop();
    }
    let last = p.last_mut()?;
    *last += 1;
    Some(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn native(name: &str) -> NativeType {
        match name {
            "int" => NativeType::Int,
            "bigint" => NativeType::BigInt,
            "smallint" => NativeType::SmallInt,
            "tinyint" => NativeType::TinyInt,
            "boolean" => NativeType::Boolean,
            "double" => NativeType::Double,
            "float" => NativeType::Float,
            "text" => NativeType::Text,
            "blob" => NativeType::Blob,
            "timestamp" => NativeType::Timestamp,
            "time" => NativeType::Time,
            "date" => NativeType::Date,
            "varint" => NativeType::Varint,
            "decimal" => NativeType::Decimal,
            "uuid" => NativeType::Uuid,
            "timeuuid" => NativeType::Timeuuid,
            "inet" => NativeType::Inet,
            other => panic!("unknown type {other}"),
        }
    }

    // Vectors generated from Lexis' server encoder (dht/ordered_encoding.cc):
    // "<types> <serialized components> <token>", comma-separated lists, hex.
    #[test]
    fn matches_server_vectors() {
        let vectors = include_str!("ordered_encoding_vectors.txt");
        let mut n = 0;
        for line in vectors.lines() {
            let mut parts = line.split(' ');
            let (types, values, token) = (
                parts.next().unwrap(),
                parts.next().unwrap(),
                parts.next().unwrap(),
            );
            // DESC columns are written desc(<type>).
            let (types, descending): (Vec<_>, Vec<_>) = types
                .split(',')
                .map(
                    |t| match t.strip_prefix("desc(").and_then(|t| t.strip_suffix(')')) {
                        Some(t) => (ColumnType::Native(native(t)), true),
                        None => (ColumnType::Native(native(t)), false),
                    },
                )
                .unzip();
            let values: Vec<_> = values.split(',').map(unhex).collect();
            let got = encode_partition_key(
                types.iter().zip(values.iter().map(|v| v.as_slice())),
                &descending,
            );
            assert_eq!(got, unhex(token), "vector: {line}");
            n += 1;
        }
        assert_eq!(n, 1030);
    }

    fn enc(typ: NativeType, raw: &[u8]) -> Vec<u8> {
        let t = ColumnType::Native(typ);
        encode_partition_key(std::iter::once((&t, raw)), &[])
    }

    #[test]
    fn desc_is_complement() {
        let t = ColumnType::Native(NativeType::Text);
        let desc = |raw: &[u8]| encode_partition_key(std::iter::once((&t, raw)), &[true]);
        let asc = enc(NativeType::Text, b"xyz");
        assert_eq!(desc(b"xyz"), asc.iter().map(|b| !b).collect::<Vec<_>>());
        // "xyz" > "xy", so DESC puts "xyz" first.
        assert!(desc(b"xyz") < desc(b"xy"));
    }

    #[test]
    fn matches_server_int_encoding() {
        // Same bytes as Lexis' partitioner_test: (int 143, int 234).
        let t = ColumnType::Native(NativeType::Int);
        let a = 143i32.to_be_bytes();
        let b = 234i32.to_be_bytes();
        let got = encode_partition_key([(&t, &a[..]), (&t, &b[..])].into_iter(), &[]);
        assert_eq!(
            got,
            vec![
                0x80, 0x00, 0x01, 0x00, 0x01, 0x8f, 0x00, 0x00, 0x80, 0x00, 0x01, 0x00, 0x01, 0xea,
                0x00, 0x00
            ]
        );
    }

    #[test]
    fn integer_order() {
        let vals = [i64::MIN, -70000, -1, 0, 1, 255, 256, i64::MAX];
        let encs: Vec<_> = vals
            .iter()
            .map(|v| enc(NativeType::BigInt, &v.to_be_bytes()))
            .collect();
        assert!(encs.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn text_with_nul_order() {
        let vals: [&[u8]; 6] = [b"", b"a", b"a\0", b"a\0b", b"ab", b"b"];
        let encs: Vec<_> = vals.iter().map(|v| enc(NativeType::Text, v)).collect();
        assert!(encs.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn varint_order() {
        // Two's complement, minimal: -129, -128, -1, 0, 1, 127, 128, 256.
        let vals: [&[u8]; 8] = [
            &[0xff, 0x7f],
            &[0x80],
            &[0xff],
            &[0x00],
            &[0x01],
            &[0x7f],
            &[0x00, 0x80],
            &[0x01, 0x00],
        ];
        let encs: Vec<_> = vals.iter().map(|v| enc(NativeType::Varint, v)).collect();
        assert!(encs.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn decimal_order_and_equality() {
        fn dec(scale: i32, unscaled: &[u8]) -> Vec<u8> {
            let mut raw = scale.to_be_bytes().to_vec();
            raw.extend_from_slice(unscaled);
            enc(NativeType::Decimal, &raw)
        }
        // -1.21 < -0.12 < -0.1 < 0 < 0.1 < 0.12 < 1 == 1.00
        let ordered = [
            dec(2, &[0xff, 0x87]), // -121 * 10^-2
            dec(2, &[0xf4]),       // -12 * 10^-2
            dec(1, &[0xff]),       // -1 * 10^-1
            dec(0, &[0x00]),
            dec(1, &[0x01]),
            dec(2, &[0x0c]),
            dec(0, &[0x01]),
        ];
        assert!(ordered.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(dec(0, &[0x01]), dec(2, &[0x64])); // 1 == 1.00
    }
}
