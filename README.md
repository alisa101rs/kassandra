# Kassandra
[![codecov](https://codecov.io/gh/alisa101rs/kassandra/graph/badge.svg?token=UZB0XQOHVP)](https://codecov.io/gh/alisa101rs/kassandra)
[![kassandra](https://img.shields.io/crates/v/kassandra.svg)](https://crates.io/crates/kassandra)
[![build](https://github.com/alisa101rs/kassandra/actions/workflows/rust.yml/badge.svg?branch=main)](https://github.com/alisa101rs/kassandra/actions/workflows/rust.yml)


This project aims to provide utilities to help testing
applications that uses cassandra as a primary database.

List of supported features:
- [x] scylla driver support
- [x] java cassandra driver support
- [x] datastax cassandra driver support
- [x] cqlsh driver
- [x] jdbc driver
- [x] same aliases support (`select name as another name`)
- [x] json support ('select json *`, `select toJson(name) as smth`)
- [x] basic queries support (create, insert/upsert, update, delete)
- [x] batch queries support
- [ ] UDTs
- [x] prepared queries support (prepare, execute, batch)
- [ ] proper system tables
- [ ] correct paging support

## Kassandra Node
[![nix](https://github.com/alisa101rs/kassandra/actions/workflows/nix.yml/badge.svg?branch=main)](https://github.com/alisa101rs/kassandra/actions/workflows/nix.yml)
[![release](https://github.com/alisa101rs/kassandra/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/alisa101rs/kassandra/actions/workflows/release.yml)

In-memory, single node database implementation
that supports cql v4 protocol.

## Kassandra Tester
[![kassandra-tester](https://img.shields.io/crates/v/kassandra-tester.svg)](https://crates.io/crates/kassandra-tester)

Provides a temporary unique socket address to connect to and run unit test.
After test is completed, returns a Kassandra instance, which then can be used
for snapshot testing.
