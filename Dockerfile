# Important: This file is provided for demonstration purposes and may NOT be suitable for production use.
# The maintainers of electrs are not deeply familiar with Docker, so you should DYOR.
# If you are not familiar with Docker either it's probably be safer to NOT use it.

FROM debian:trixie-slim AS base
RUN apt-get update -qqy
RUN apt-get install -qqy librocksdb-dev curl

### Electrum Rust Server ###
FROM base AS electrs-build
RUN apt-get install -qqy cargo build-essential libclang-dev git capnproto libcapnp-dev

# Install electrs (with sibling bitcoin-capnp-types checkout for the
# experimental chain-interface branch -- see Cargo.toml path dep).
WORKDIR /build/capnp-types
RUN git clone --depth 1 --branch 2026/04/chain-interface \
      https://github.com/Sjors/bitcoin-capnp-types.git .
WORKDIR /build/electrs
COPY . .
ENV ROCKSDB_INCLUDE_DIR=/usr/include
ENV ROCKSDB_LIB_DIR=/usr/lib
RUN cargo install --locked --path .

FROM base AS result
# Copy the binaries
COPY --from=electrs-build /root/.cargo/bin/electrs /usr/bin/electrs

WORKDIR /
