FROM rust:1.97-bookworm AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends cmake \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p peren

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --system --home /var/lib/peren --create-home peren \
  && mkdir -p /etc/peren /var/lib/peren \
  && chown -R peren:peren /var/lib/peren
COPY --from=build /src/target/release/peren /usr/local/bin/peren
USER peren
WORKDIR /var/lib/peren
EXPOSE 7000 8080
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 CMD ["peren", "status", "/etc/peren/config.toml"]
ENTRYPOINT ["peren"]
CMD ["serve", "/etc/peren/config.toml"]
