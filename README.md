# Kassandra

This project aims to provide utilities to help testing
applications that uses cassandra as a primary database.

List of supported features:
- [x] scylla driver support
- [x] java cassandra driver support
- [x] datastax cassandra driver support
- [ ] other drivers (cqlsh, intelij driver)
- [x] basic queries support (create, insert/upsert, update, delete)
- [x] batch queries support
- [ ] UDTs
- [x] prepared queries support (prepare, execute, batch)
- [ ] proper system tables
- [ ] correct paging support

## Kassandra Node

In-memory, single node database implementation
that supports cql v4 protocol.

## Kassandra Tester

Provides a temporary unique socket address to connect to and run unit test.
After test is completed, returns a Kassandra instance, which then can be used
for snapshot testing.
