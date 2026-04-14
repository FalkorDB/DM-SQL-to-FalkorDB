# syntax=docker/dockerfile:1.7

FROM node:22-bookworm AS ui-build
WORKDIR /src/control-plane/ui
COPY control-plane/ui/package.json control-plane/ui/package-lock.json ./
RUN npm ci
COPY control-plane/ui/ ./
RUN npm run build

FROM rust:1.87-bookworm AS server-build
WORKDIR /src
COPY . ./
RUN cargo build --manifest-path control-plane/server/Cargo.toml --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
ARG KUBECTL_VERSION=v1.33.1
RUN ARCH="$(dpkg --print-architecture)" \
    && case "${ARCH}" in \
      amd64) KARCH="amd64" ;; \
      arm64) KARCH="arm64" ;; \
      *) echo "unsupported architecture: ${ARCH}" && exit 1 ;; \
    esac \
    && curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${KARCH}/kubectl" -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser

WORKDIR /opt/falkordb

COPY --from=server-build /src/control-plane/server/target/release/falkordb-migrate-control-plane /usr/local/bin/falkordb-migrate-control-plane
COPY --from=server-build /src /opt/falkordb/repo
COPY --from=ui-build /src/control-plane/ui/dist /opt/falkordb/repo/control-plane/ui/dist

ENV CONTROL_PLANE_REPO_ROOT=/opt/falkordb/repo
ENV CONTROL_PLANE_UI_DIST=/opt/falkordb/repo/control-plane/ui/dist
ENV CONTROL_PLANE_DATA_DIR=/var/lib/falkordb-control-plane
ENV CONTROL_PLANE_EXECUTION_BACKEND=kubernetes
ENV CONTROL_PLANE_BIND=0.0.0.0:3003

RUN mkdir -p /var/lib/falkordb-control-plane/runs /var/lib/falkordb-control-plane/configs \
    && chown -R appuser:appuser /opt/falkordb /var/lib/falkordb-control-plane

USER appuser
EXPOSE 3003

ENTRYPOINT ["/usr/local/bin/falkordb-migrate-control-plane"]
