# syntax=docker/dockerfile:1.7

FROM node:22-bookworm AS ui-build
WORKDIR /src/control-plane/ui
COPY control-plane/ui/package.json control-plane/ui/package-lock.json ./
RUN npm ci
COPY control-plane/ui/ ./
RUN npm run build

FROM rust:1.96-bookworm AS server-build
WORKDIR /src
COPY . ./
RUN cargo build --manifest-path control-plane/server/Cargo.toml --release

# Build kubectl from source instead of downloading the upstream release binary.
# The upstream binary is built against whatever Go toolchain and Go module
# versions the Kubernetes release pinned, which lag behind security fixes
# (stdlib, golang.org/x/net, golang.org/x/text). Building it here lets us use a
# patched toolchain and patched modules on top of the same release tag.
#
# When bumping these, keep in mind:
#   - The golang image tag must be a Go release that includes the stdlib
#     security fixes. GOTOOLCHAIN=local makes the build fail instead of
#     silently downloading a different toolchain if the kubectl release
#     requires a newer one.
#   - golang.org/x/net v0.62.0 itself requires golang.org/x/text >= v0.43.0, so
#     the two module pins must be bumped together.
FROM golang:1.26.11-bookworm AS kubectl-build
ARG KUBECTL_VERSION=v1.36.6
ARG GOLANG_X_NET_VERSION=v0.62.0
ARG GOLANG_X_TEXT_VERSION=v0.43.0
WORKDIR /src
RUN git clone --depth 1 --branch "${KUBECTL_VERSION}" https://github.com/kubernetes/kubernetes.git .
# The Kubernetes repository ships a go.work file; workspace mode does not allow
# -mod=mod, so it is disabled and the root go.mod (with its staging replaces)
# is used directly.
ENV GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=mod CGO_ENABLED=0
RUN go get "golang.org/x/net@${GOLANG_X_NET_VERSION}" "golang.org/x/text@${GOLANG_X_TEXT_VERSION}" \
    && go build -trimpath \
        -ldflags "-s -w -X k8s.io/component-base/version.gitVersion=${KUBECTL_VERSION} -X k8s.io/component-base/version.gitTreeState=clean" \
        -o /out/kubectl ./cmd/kubectl

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=kubectl-build /out/kubectl /usr/local/bin/kubectl

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
