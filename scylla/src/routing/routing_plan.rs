//! Lexis prefix routing (`LEXIS_PREFIX_ROUTING_V1`).
//!
//! A PREPARED response carries, in its `lexis-prefix-routing-v1` custom
//! payload, a plan for computing from the bound values the token the
//! statement's partition ranges start at. It lets partition key prefix and
//! range reads (`WHERE tenant = ?`, `WHERE tenant = ? AND user > ?`, `token()`
//! ranges) be routed, and carries the partition key directions, so
//! complete-key reads of DESC tables are routed without schema metadata.
//! It mirrors the server's `statement_restrictions::lexis_routing_plan()` and
//! `get_pk_ordered_prefix_ranges()`.

use bytes::Bytes;
use scylla_cql::deserialize::FrameSlice;
use scylla_cql::deserialize::value::DeserializeValue;
use scylla_cql::frame::response::result::{CollectionType, ColumnType, NativeType};
use scylla_cql::frame::types::RawValue;
use scylla_cql::serialize::row::SerializedValues;
use std::sync::LazyLock;

use crate::routing::Token;
use crate::routing::ordered_encoding::{encode_partition_key, prefix_successor};

pub(crate) const CUSTOM_PAYLOAD_KEY: &str = "lexis-prefix-routing-v1";

const EQ: i8 = 0;
const IN: i8 = 1;
const IN_LIST: i8 = 2;
const SLICE: i8 = 3;
const TOKEN: i8 = 4;
const UPPER: i8 = 1;
const INCLUSIVE: i8 = 2;

// list<tuple<text, boolean, tinyint, list<tuple<int, blob, tinyint>>>>
static PLAN_TYPE: LazyLock<ColumnType<'static>> = LazyLock::new(|| {
    let list = |t| ColumnType::Collection {
        frozen: false,
        typ: CollectionType::List(Box::new(t)),
    };
    list(ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::Text),
        ColumnType::Native(NativeType::Boolean),
        ColumnType::Native(NativeType::TinyInt),
        list(ColumnType::Tuple(vec![
            ColumnType::Native(NativeType::Int),
            ColumnType::Native(NativeType::Blob),
            ColumnType::Native(NativeType::TinyInt),
        ])),
    ]))
});

type RawPlan = Vec<(String, bool, i8, Vec<(i32, Vec<u8>, i8)>)>;

/// The encoder only needs the native type; anything else is encoded as its
/// serialized bytes, like a blob.
fn native_type(name: &str) -> NativeType {
    match name {
        "ascii" => NativeType::Ascii,
        "bigint" => NativeType::BigInt,
        "boolean" => NativeType::Boolean,
        "counter" => NativeType::Counter,
        "date" => NativeType::Date,
        "decimal" => NativeType::Decimal,
        "double" => NativeType::Double,
        "duration" => NativeType::Duration,
        "float" => NativeType::Float,
        "inet" => NativeType::Inet,
        "int" => NativeType::Int,
        "smallint" => NativeType::SmallInt,
        "text" | "varchar" => NativeType::Text,
        "time" => NativeType::Time,
        "timestamp" => NativeType::Timestamp,
        "timeuuid" => NativeType::Timeuuid,
        "tinyint" => NativeType::TinyInt,
        "uuid" => NativeType::Uuid,
        "varint" => NativeType::Varint,
        _ => NativeType::Blob,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Value {
    marker: i32,
    literal: Vec<u8>,
    flags: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    typ: ColumnType<'static>,
    descending: bool,
    op: i8,
    values: Vec<Value>,
}

/// A parsed `lexis-prefix-routing-v1` plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutingPlan {
    entries: Vec<Entry>,
}

/// The elements of a serialized CQL list, or None if malformed or one is null.
fn list_elements(serialized: &[u8]) -> Option<Vec<&[u8]>> {
    let read_i32 = |b: &[u8], pos: usize| -> Option<i32> {
        Some(i32::from_be_bytes(b.get(pos..pos + 4)?.try_into().ok()?))
    };
    let count = read_i32(serialized, 0)?;
    let mut pos = 4;
    let mut elements = Vec::new();
    for _ in 0..count {
        let len = usize::try_from(read_i32(serialized, pos)?).ok()?;
        pos += 4;
        elements.push(serialized.get(pos..pos + len)?);
        pos += len;
    }
    Some(elements)
}

impl RoutingPlan {
    pub(crate) fn from_payload(payload: &Bytes) -> Option<Self> {
        <RawPlan as DeserializeValue<'_, '_>>::type_check(&PLAN_TYPE).ok()?;
        let raw: RawPlan = <RawPlan as DeserializeValue<'_, '_>>::deserialize(
            &PLAN_TYPE,
            Some(FrameSlice::new(payload)),
        )
        .ok()?;
        let entries = raw
            .into_iter()
            .map(|(typ, descending, op, values)| Entry {
                typ: ColumnType::Native(native_type(&typ)),
                descending,
                op,
                values: values
                    .into_iter()
                    .map(|(marker, literal, flags)| Value {
                        marker,
                        literal,
                        flags,
                    })
                    .collect(),
            })
            .collect();
        Some(Self { entries })
    }

    /// The token the statement's partition ranges start at, or None if it
    /// cannot be computed from these values.
    pub(crate) fn start_token(&self, values: &SerializedValues) -> Option<Token> {
        let bound: Vec<Option<&[u8]>> = values
            .iter()
            .map(|v| match v {
                RawValue::Value(v) => Some(v),
                _ => None,
            })
            .collect();
        let value = |v: &Value| -> Option<Vec<u8>> {
            if v.marker < 0 {
                return Some(v.literal.clone());
            }
            bound
                .get(usize::try_from(v.marker).ok()?)
                .copied()
                .flatten()
                .map(<[u8]>::to_vec)
        };
        let token = |b: Vec<u8>| Token::from_ordered_bytes(b.into());

        if let Some(first) = self.entries.first()
            && first.op == TOKEN
        {
            let mut start = Vec::new();
            for v in first.values.iter().filter(|v| v.flags & UPPER == 0) {
                start = value(v)?;
                if v.flags & INCLUSIVE == 0 {
                    start.push(0x00);
                }
            }
            return Some(token(start));
        }

        let mut types = Vec::new();
        let mut descending = Vec::new();
        let mut choices: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut range_column = None;
        for entry in &self.entries {
            let options = match entry.op {
                SLICE => {
                    range_column = Some(entry);
                    continue;
                }
                EQ | IN => entry.values.iter().map(value).collect::<Option<Vec<_>>>()?,
                IN_LIST => list_elements(&value(entry.values.first()?)?)?
                    .into_iter()
                    .map(<[u8]>::to_vec)
                    .collect(),
                _ => return None,
            };
            types.push(entry.typ.clone());
            descending.push(entry.descending);
            choices.push(options);
        }

        let encode = |types: &[ColumnType<'static>], components: &[&[u8]], desc: &[bool]| {
            encode_partition_key(types.iter().zip(components.iter().copied()), desc)
        };
        let mut best: Option<Vec<u8>> = None;
        // As get_pk_ordered_prefix_ranges(): one range per prefix combination.
        let mut combo = vec![0usize; choices.len()];
        'combos: loop {
            if choices.iter().any(|c| c.is_empty()) {
                break;
            }
            let components: Vec<&[u8]> = combo
                .iter()
                .zip(&choices)
                .map(|(&i, c)| c[i].as_slice())
                .collect();
            let p = (!components.is_empty()).then(|| encode(&types, &components, &descending));
            let mut lo = p.clone();
            let mut hi = p.and_then(prefix_successor);
            let mut skip = false;
            if let Some(entry) = range_column {
                let mut slice_types = types.clone();
                slice_types.push(entry.typ.clone());
                let mut slice_desc = descending.clone();
                slice_desc.push(entry.descending);
                for v in &entry.values {
                    let b = value(v)?;
                    let mut c = components.clone();
                    c.push(&b);
                    let e = encode(&slice_types, &c, &slice_desc);
                    let inclusive = v.flags & INCLUSIVE != 0;
                    // A DESC column encodes in the opposite order: its CQL
                    // upper bound is the low end of the range.
                    if (v.flags & UPPER != 0) == entry.descending {
                        lo = if inclusive {
                            Some(e)
                        } else {
                            prefix_successor(e)
                        };
                        if lo.is_none() {
                            skip = true;
                        }
                    } else {
                        hi = if inclusive {
                            prefix_successor(e)
                        } else {
                            Some(e)
                        };
                    }
                }
            }
            let empty = matches!((&lo, &hi), (Some(lo), Some(hi)) if hi <= lo);
            if !skip && !empty {
                let start = lo.unwrap_or_default();
                if best.as_ref().is_none_or(|b| start < *b) {
                    best = Some(start);
                }
            }
            // Next combination.
            for i in (0..combo.len()).rev() {
                combo[i] += 1;
                if combo[i] < choices[i].len() {
                    continue 'combos;
                }
                combo[i] = 0;
            }
            break;
        }
        best.map(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla_cql::frame::response::result::ColumnType;

    /// (type name, descending, operation, [(marker, literal, flags)])
    type TestEntry<'a> = (&'a str, bool, i8, Vec<(i32, &'a [u8], i8)>);

    fn plan(entries: Vec<TestEntry<'_>>) -> RoutingPlan {
        RoutingPlan {
            entries: entries
                .into_iter()
                .map(|(t, descending, op, values)| Entry {
                    typ: ColumnType::Native(native_type(t)),
                    descending,
                    op,
                    values: values
                        .into_iter()
                        .map(|(marker, literal, flags)| Value {
                            marker,
                            literal: literal.to_vec(),
                            flags,
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    fn values(vs: &[&[u8]]) -> SerializedValues {
        let mut s = SerializedValues::new();
        for v in vs {
            s.add_value(&v.to_vec(), &ColumnType::Native(NativeType::Blob))
                .unwrap();
        }
        s
    }

    fn enc(types: &[(&str, bool)], components: &[&[u8]]) -> Vec<u8> {
        let t: Vec<_> = types
            .iter()
            .map(|(t, _)| ColumnType::Native(native_type(t)))
            .collect();
        let d: Vec<_> = types.iter().map(|(_, d)| *d).collect();
        encode_partition_key(t.iter().zip(components.iter().copied()), &d)
    }

    fn bytes_of(t: Option<Token>) -> Option<Vec<u8>> {
        t.map(|t| t.ordered_bytes().unwrap().to_vec())
    }

    #[test]
    fn prefix_eq_and_desc_range() {
        let five = 5i32.to_be_bytes();
        let nine = 9i32.to_be_bytes();
        let p = plan(vec![("text", false, EQ, vec![(0, b"", 0)])]);
        assert_eq!(
            bytes_of(p.start_token(&values(&[b"b"]))),
            Some(enc(&[("text", false)], &[b"b"]))
        );
        // DESC: the CQL upper bound is the low end.
        let p = plan(vec![
            ("text", false, EQ, vec![(0, b"", 0)]),
            (
                "int",
                true,
                SLICE,
                vec![(1, b"", INCLUSIVE), (2, b"", UPPER | INCLUSIVE)],
            ),
        ]);
        assert_eq!(
            bytes_of(p.start_token(&values(&[b"b", &five, &nine]))),
            Some(enc(&[("text", false), ("int", true)], &[b"b", &nine]))
        );
        // Empty range.
        assert_eq!(p.start_token(&values(&[b"b", &nine, &five])), None);
    }

    #[test]
    fn ascending_exclusive_bound_and_in() {
        let five = 5i32.to_be_bytes();
        let p = plan(vec![
            ("text", false, IN, vec![(0, b"", 0), (1, b"", 0)]),
            ("int", false, SLICE, vec![(2, b"", 0)]),
        ]);
        let e = enc(&[("text", false), ("int", false)], &[b"c", &five]);
        assert_eq!(
            bytes_of(p.start_token(&values(&[b"z", b"c", &five]))),
            prefix_successor(e)
        );
    }

    #[test]
    fn in_list_and_token_range() {
        let list = [
            &2i32.to_be_bytes()[..],
            &1i32.to_be_bytes(),
            b"c",
            &1i32.to_be_bytes(),
            b"z",
        ]
        .concat();
        let p = plan(vec![("text", true, IN_LIST, vec![(0, b"", 0)])]);
        assert_eq!(
            bytes_of(p.start_token(&values(&[&list]))),
            Some(enc(&[("text", true)], &[b"z"]))
        );
        let p = plan(vec![(
            "blob",
            false,
            TOKEN,
            vec![(0, b"", 0), (1, b"", UPPER)],
        )]);
        assert_eq!(
            bytes_of(p.start_token(&values(&[b"\x80", b"\x90"]))),
            Some(b"\x80\x00".to_vec())
        );
    }
}
