# lexis-rust-driver

<table><tr>
<td valign="middle" width="160"><img src="https://raw.githubusercontent.com/jk89/lexisdb/develop/lexis-icon.svg" width="160" height="196" alt="Lexis"></td>
<td valign="middle">

The Rust driver for [Lexis](https://github.com/jk89/lexisdb), a fork of ScyllaDB
that replaces the Murmur3 partitioner with byte-ordered partitioning, built on
tablets. In Lexis the token of a partition is an order-preserving encoding of
its partition key.

</td>
</tr></table>

With a Lexis cluster, the driver computes these tokens on the client, so each
query is sent to the node and shard that own its partition, and it follows
Lexis tablet routing. `token()` values are blobs, read as `Vec<u8>`. With
Cassandra and ScyllaDB clusters the driver behaves as upstream.

## Installation

In `Cargo.toml`:

```toml
[dependencies]
scylla = { git = "https://github.com/jk89/lexis-rust-driver" }
```

## Base and licence

Forked from the ScyllaDB Rust driver at tag `v1.6.0`. Licensed under either
the Apache License 2.0 ([LICENSE-APACHE](LICENSE-APACHE)) or the MIT licence
([LICENSE-MIT](LICENSE-MIT)), at your option.

The upstream README is kept unchanged in [README_SCYLLA.md](README_SCYLLA.md).