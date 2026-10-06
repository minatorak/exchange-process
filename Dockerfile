# syntax=docker/dockerfile:1
# CTX: Locked multi-stage build. Copies only the release binary into a pinned
#      minimal runtime and runs non-root. Inbound transport is the Kafka
#      consumer; the only listener is the health endpoint (ADR-0004).
#
#      Build context is this repository root, INCLUDING the vendored
#      third_party/bybit-rs submodule (init before building):
#        git submodule update --init
#        docker build -t exchange-process:dev .
#
#      Contract satisfied here (trading-infra/docs/service-onboarding.md):
#        - mounted config binds 0.0.0.0, never 127.0.0.1
#        - configuration and secrets arrive at runtime; neither is baked in
#        - no password or .env baked into the image
#        - STOPSIGNAL SIGTERM so Kubernetes can terminate cleanly
#        - health port is declared and distinct from other services
# RULE:
#   - NEVER copy .env, .env.local, secrets, config.toml, or the source tree
#     into the runtime stage
#   - NEVER run as root
#   - NEVER add DATABASE_URL, KAFKA_BOOTSTRAP, CREDENTIAL_DECRYPT_KEY or any
#     credential as an ENV here; the only ENV is the config file path
#   - NEVER use `imagePullPolicy: Always` with the local tag this image is
#     built under

FROM docker.io/library/rust:1.98-bookworm AS builder

WORKDIR /workspace

# Image-only release profile: strip symbols and let thin LTO drop unused code.
# Local `cargo build --release` keeps Cargo.toml's profile.
ENV CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_PROFILE_RELEASE_LTO=thin \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1

# rdkafka's cmake-build feature compiles librdkafka statically; the rust
# image ships gcc/g++ and make but not cmake.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY third_party/bybit-rs ./third_party/bybit-rs

# Cache mounts persist downloaded crates and compiled dependencies across
# builds. Per-project cache ids: concurrent builds sharing one registry mount
# race on crate extraction, because cargo's registry lock file lives outside
# the mount.
RUN --mount=type=cache,id=exchange-process-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=exchange-process-target,target=/workspace/target \
    cargo build --locked --release \
    && cp target/release/exchange-process /usr/local/bin/exchange-process

# distroless/cc ships glibc, libgcc, CA certificates and tzdata — all a
# rustls binary needs — with no shell or package manager.
FROM gcr.io/distroless/cc-debian12:nonroot
# :nonroot defaults to /home/nonroot, which uid 10001 cannot enter.
WORKDIR /
COPY --from=builder /usr/local/bin/exchange-process /usr/local/bin/exchange-process
ENV EXCHANGE_PROCESS_CONFIG=/etc/exchange-process/config.toml
EXPOSE 8090
USER 10001:10001
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/exchange-process"]
