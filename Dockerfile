# Multi-stage build for pamsoft_grid_operator (Rust).
# The operator depends on two git crates: `pamsoft_grid` (image algorithm,
# brings OpenCV bindings) and `tercen-rs` (Tercen gRPC SDK, brings tonic +
# polars). The build stage therefore needs `git` plus OpenCV 4 + clang for
# the `opencv` crate bindings used by pamsoft_grid.

# ============================================================================
# Builder stage
# ============================================================================
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        pkg-config \
        clang \
        libclang-dev \
        libopencv-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Prime the dep cache: dummy source stubs for every declared target in
# Cargo.toml (lib + two bins) let the first `cargo build` fetch all git
# deps and compile their heavy transitive stacks (OpenCV via pamsoft_grid,
# tonic/polars via tercen-rs) into a Docker layer that only invalidates
# when Cargo.{toml,lock} change.
#
# The `--mount=type=secret,id=gh_pat` exposes a GitHub PAT (read-only
# access to tercen/pamsoft_grid_rust) for the duration of this RUN, so
# cargo can clone the private git dep. The secret is NOT baked into the
# image. Pass from docker build with: --secret id=gh_pat,env=GH_PAT.
# Build locally against a public repo without a secret by omitting the
# flag — the rewrite step is skipped.
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=secret,id=gh_pat \
    if [ -s /run/secrets/gh_pat ]; then \
      GH_PAT=$(cat /run/secrets/gh_pat) && \
      git config --global url."https://${GH_PAT}@github.com/".insteadOf "https://github.com/"; \
    fi && \
    mkdir -p src/bin && \
    : > src/lib.rs && \
    echo 'fn main() {}' > src/main.rs && \
    echo 'fn main() {}' > src/bin/dev.rs && \
    cargo build --release && \
    rm -rf src && \
    cargo clean -p pamsoft_grid_operator --release
# `cargo clean -p` wipes all artifacts for the local crate (lib +
# every bin) so the second build re-derives them from the real
# sources. Necessary because Cargo's per-crate cache keys
# `lib<name>-HASH.rmeta` distinct from `<name>-HASH.{d,rlib}` —
# a bare `rm -rf target/release/deps/pamsoft_grid_operator*` would
# miss the `lib*` files and the next build would happily reuse the
# stale empty-lib metadata.

# Real sources. The deps layer above already fetched git deps so the
# git-auth config isn't needed here. Build only the production binary —
# the `dev` binary is for local use and doesn't need to ship in the image.
COPY src ./src
# operator.json is embedded at compile time by src/props.rs via
# `include_str!("../operator.json")` — the property defaults live there
# as a single source of truth shared with Tercen.
COPY operator.json ./
RUN cargo build --release --bin pamsoft_grid_operator

# ============================================================================
# Runtime stage
# ============================================================================
FROM debian:bookworm-slim

# libopencv-dev is the simplest way to pull in all OpenCV shared libs the
# binary depends on (imgproc, imgcodecs, features2d, core). Can be trimmed
# to specific libopencv-*406 runtime packages once a stable set is pinned.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libtiff6 \
        libopencv-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/pamsoft_grid_operator /usr/local/bin/pamsoft_grid_operator

WORKDIR /operator
ENV RUST_BACKTRACE=1

ENTRYPOINT ["/usr/local/bin/pamsoft_grid_operator"]
