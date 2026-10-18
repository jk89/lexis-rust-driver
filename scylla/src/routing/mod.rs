//! This module holds entities whose goal is to enable routing requests optimally,
//! that is, choosing a target node and a shard such that it is a replica for
//! given token.
//!
//! This includes:
//! - token representation,
//! - shard representation and shard computing logic,
//! - partitioners, which compute token based on a partition key,
//! - replica locator, which finds replicas (node + shard) for a given token.
//!

pub mod locator;
pub(crate) mod ordered_encoding;
pub mod partitioner;
mod sharding;

pub use sharding::{InvalidShardAwarePortRange, Shard, ShardAwarePortRange, ShardCount, Sharder};
pub(crate) use sharding::{ShardInfo, ShardingError};

/// Token identifies the position of a partition in the token space.
///
/// With Murmur3Partitioner and CDCPartitioner a token is basically an i64 with
/// one caveat: i64::MIN is not a valid token. It is used to represent infinity.
/// For this reason tokens are normalized - i64::MIN is replaced with i64::MAX.
/// See this fragment of ScyllaDB code for more information:
/// <https://github.com/scylladb/scylladb/blob/4be70bfc2bc7f133cab492b4aac7bab9c790a48c/dht/token.hh#L32>
///
/// With Lexis' OrderedPartitioner a token is the order-preserving encoding of
/// the partition key, compared as an unsigned byte string. Tokens of the two
/// kinds are never mixed within one table.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Hash)]
pub struct Token {
    repr: TokenRepr,
}

// Variant order is token order: ordered tokens sort below OrderedMax, which
// stands for the end of the ordered token space.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Hash)]
enum TokenRepr {
    Long(i64),
    Ordered(bytes::Bytes),
    OrderedMax,
}

impl Token {
    /// Creates a new token with given value, normalizing the value if necessary.
    #[inline]
    pub fn new(value: i64) -> Self {
        Self {
            repr: TokenRepr::Long(if value == i64::MIN { i64::MAX } else { value }),
        }
    }

    /// Creates an ordered (OrderedPartitioner) token from its bytes.
    #[inline]
    pub fn from_ordered_bytes(bytes: bytes::Bytes) -> Self {
        Self {
            repr: TokenRepr::Ordered(bytes),
        }
    }

    /// The token after every ordered token.
    pub(crate) const ORDERED_MAX: Self = Token {
        repr: TokenRepr::OrderedMax,
    };

    /// Invalid Token - contains i64::MIN as value.
    ///
    /// This is (currently) only required by CDCPartitioner.
    /// See the following comment:
    /// https://github.com/scylladb/scylla-rust-driver/blob/049dc3546d24e45106fed0fdb985ec2511ab5192/scylla/src/transport/partitioner.rs#L312-L322
    pub(crate) const INVALID: Self = Token {
        repr: TokenRepr::Long(i64::MIN),
    };

    /// Retrieves the value of the token.
    ///
    /// For an ordered token this is the 64-bit projection ScyllaDB uses for
    /// static sharding: the first 8 bytes, big-endian and zero-padded, with the
    /// top bit flipped so that signed order matches token order.
    #[inline]
    pub fn value(&self) -> i64 {
        match &self.repr {
            TokenRepr::Long(v) => *v,
            TokenRepr::Ordered(b) => {
                let mut prefix = [0u8; 8];
                let n = b.len().min(8);
                prefix[..n].copy_from_slice(&b[..n]);
                (u64::from_be_bytes(prefix) ^ (1u64 << 63)) as i64
            }
            TokenRepr::OrderedMax => i64::MAX,
        }
    }

    /// Returns the bytes of an ordered token.
    #[inline]
    pub fn ordered_bytes(&self) -> Option<&[u8]> {
        match &self.repr {
            TokenRepr::Ordered(b) => Some(b),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Token;
    use bytes::Bytes;

    fn ordered(b: &'static [u8]) -> Token {
        Token::from_ordered_bytes(Bytes::from_static(b))
    }

    #[test]
    fn ordered_token_order() {
        // Unsigned byte order, then length; ORDERED_MAX after everything.
        let tokens = [
            ordered(b""),
            ordered(&[0x00]),
            ordered(&[0x00, 0x00]),
            ordered(&[0x7f]),
            ordered(&[0x80]),
            ordered(&[0xff, 0xff]),
            Token::ORDERED_MAX,
        ];
        assert!(tokens.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn ordered_token_value_is_server_projection() {
        // ScyllaDB's to_int64(): first 8 bytes big-endian, zero-padded, top bit flipped.
        assert_eq!(ordered(b"").value(), i64::MIN);
        assert_eq!(ordered(&[0x80]).value(), 0);
        assert_eq!(ordered(&[0x80, 0, 0, 0, 0, 0, 0, 0x2a, 0x99]).value(), 42);
        assert_eq!(ordered(&[0xff; 9]).value(), i64::MAX);
        // Monotonic in token order.
        assert!(ordered(&[0x10]).value() <= ordered(&[0x10, 0x00, 0x01]).value());
    }
}
