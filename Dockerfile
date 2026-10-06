# syntax=docker/dockerfile:1
FROM docker.io/library/rust:1.98-bookworm AS builder

WORKDIR /build

# Image-only release profile: strip symbols and let thin LTO drop unused code
# across crates. Local `cargo build --release` keeps Cargo.toml's profile.
ENV CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_PROFILE_RELEASE_LTO=thin \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Cache mounts persist downloaded crates and compiled dependencies across
# builds, so a source-only change recompiles just the changed crates. The
# binary is copied out of the cache mount into the image layer. Per-project
# cache ids: concurrent builds sharing one registry mount race on crate
# extraction, because cargo's registry lock file lives outside the mount.
RUN --mount=type=cache,id=exchange-process-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=exchange-process-target,target=/build/target \
    cargo build --release --locked -p exchange-process-app --bin exchange-process \
    && cp target/release/exchange-process /usr/local/bin/exchange-process

# distroless/cc ships glibc, libgcc, CA certificates and tzdata — all a rustls
# binary needs — with no shell or package manager.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

WORKDIR /app

COPY --from=builder /usr/local/bin/exchange-process /usr/local/bin/exchange-process

USER 10001:10001

# No EXPOSE: this process opens no listener (ADR-0002).

ENTRYPOINT ["exchange-process"]
