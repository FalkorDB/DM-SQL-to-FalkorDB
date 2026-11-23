# syntax=docker/dockerfile:1.7

FROM rust:1.96-bookworm AS builder
WORKDIR /src
COPY . ./

RUN cargo build --manifest-path BigQuery-to-FalkorDB/bigquery-to-falkordb/Cargo.toml --release \
    && cargo build --manifest-path ClickHouse-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path Databricks-to-FalkorDB/databricks-to-falkordb/Cargo.toml --release \
    && cargo build --manifest-path Iceberg-to-FalkorDB/iceberg-to-falkordb/Cargo.toml --release \
    && cargo build --manifest-path MariaDB-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path MySQL-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path Oracle-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path Parquet-to-FalkorDB/parquet-to-falkordb/Cargo.toml --release \
    && cargo build --manifest-path PostgreSQL-to-FalkorDB/postgres-to-falkordb/Cargo.toml --release \
    && cargo build --manifest-path SQLServer-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path Snowflake-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path Spark-to-FalkorDB/spark-to-falkordb/Cargo.toml --release

# Download and unpack Oracle Instant Client in a throwaway stage so the download
# tooling (wget, unzip) never lands in the runtime image.
FROM debian:bookworm-slim AS oracle-instantclient
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates unzip wget \
    && rm -rf /var/lib/apt/lists/*

# The Oracle-to-FalkorDB loader uses the `oracle` crate (ODPI-C), which dynamically loads
# libclntsh.so from Oracle Instant Client at runtime. Without it, Oracle mappings fail with
# "DPI-1047: Cannot locate a 64-bit Oracle Client library". See:
# https://oracle.github.io/odpi/doc/installation.html#linux
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) IC_ZIP="instantclient-basiclite-linuxx64.zip" ;; \
      arm64) IC_ZIP="instantclient-basiclite-linux-arm64.zip" ;; \
      *) echo "Unsupported architecture for Oracle Instant Client: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    mkdir -p /opt/oracle; \
    cd /opt/oracle; \
    wget -q --https-only "https://download.oracle.com/otn_software/linux/instantclient/${IC_ZIP}"; \
    unzip -q "${IC_ZIP}"; \
    rm -f "${IC_ZIP}"; \
    IC_DIR="$(find /opt/oracle -maxdepth 1 -type d -name 'instantclient_*' | head -n1)"; \
    test -n "${IC_DIR}"; \
    mv "${IC_DIR}" /opt/oracle/instantclient; \
    ls /opt/oracle/instantclient/libclntsh.so*

FROM debian:bookworm-slim AS runtime

# libaio1 is required by Oracle Instant Client. Nothing else is installed: the runner
# only needs glibc, CA certificates and the migration binaries.
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates libaio1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=oracle-instantclient --chown=0:0 /opt/oracle/instantclient /opt/oracle/instantclient
RUN echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig \
    && ldconfig -p | grep -q libclntsh
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient

# Strip setuid/setgid bits: nothing in the runner needs to change identity.
RUN groupadd --gid 10002 runner \
    && useradd --create-home --uid 10002 --gid 10002 --shell /usr/sbin/nologin runner \
    && mkdir -p /opt/falkordb/bin /workspace \
    && chown runner:runner /workspace \
    && find / -xdev -perm /6000 -type f -exec chmod a-s {} +

COPY --from=builder --chown=0:0 /src/BigQuery-to-FalkorDB/bigquery-to-falkordb/target/release/bigquery-to-falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/ClickHouse-to-FalkorDB/target/release/clickhouse_to_falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/Databricks-to-FalkorDB/databricks-to-falkordb/target/release/databricks-to-falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/Iceberg-to-FalkorDB/iceberg-to-falkordb/target/release/iceberg-to-falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/MariaDB-to-FalkorDB/target/release/mariadb_to_falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/MySQL-to-FalkorDB/target/release/mysql_to_falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/Oracle-to-FalkorDB/target/release/oracle_to_falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/Parquet-to-FalkorDB/parquet-to-falkordb/target/release/parquet-to-falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/PostgreSQL-to-FalkorDB/postgres-to-falkordb/target/release/postgres-to-falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/SQLServer-to-FalkorDB/target/release/sqlserver_to_falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/Snowflake-to-FalkorDB/target/release/snowflake_to_falkordb /opt/falkordb/bin/
COPY --from=builder --chown=0:0 /src/Spark-to-FalkorDB/spark-to-falkordb/target/release/spark-to-falkordb /opt/falkordb/bin/

ENV PATH=/opt/falkordb/bin:${PATH}

WORKDIR /workspace
# Numeric UID/GID so Kubernetes can enforce `runAsNonRoot` without an explicit runAsUser.
USER 10002:10002
CMD ["sleep", "infinity"]
