#!/usr/bin/env just --justfile
set dotenv-load

root := justfile_directory()

cargo-fix-all:
    cargo fix --allow-dirty --allow-staged --all
    cargo clippy --fix --allow-dirty --allow-staged --all-targets --all-features


run-kassandra:
    cargo run --package kassandra-node

all-tests:
    cargo test --package kassandra

publish:
    cd {{root}}/kassandra && cargo publish
    cd {{root}}/kassandra-tester && cargo publish
