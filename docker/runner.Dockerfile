# syntax=docker/dockerfile:1.7

FROM rust:1.87-bookworm AS builder
WORKDIR /src
COPY . ./

RUN cargo build --manifest-path BigQuery-to-FalkorDB/bigquery-to-falkordb/Cargo.toml --release \
    && cargo build --manifest-path ClickHouse-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path Databricks-to-FalkorDB/databricks-to-falkordb/Cargo.toml --release \
    && cargo build --manifest-path MariaDB-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path MySQL-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path PostgreSQL-to-FalkorDB/postgres-to-falkordb/Cargo.toml --release \
    && cargo build --manifest-path SQLServer-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path Snowflake-to-FalkorDB/Cargo.toml --release \
    && cargo build --manifest-path Spark-to-FalkorDB/spark-to-falkordb/Cargo.toml --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10002 --shell /usr/sbin/nologin runner
RUN mkdir -p /opt/falkordb/bin /workspace \
    && chown -R runner:runner /opt/falkordb /workspace

COPY --from=builder /src/BigQuery-to-FalkorDB/bigquery-to-falkordb/target/release/bigquery-to-falkordb /opt/falkordb/bin/
COPY --from=builder /src/ClickHouse-to-FalkorDB/target/release/clickhouse_to_falkordb /opt/falkordb/bin/
COPY --from=builder /src/Databricks-to-FalkorDB/databricks-to-falkordb/target/release/databricks-to-falkordb /opt/falkordb/bin/
COPY --from=builder /src/MariaDB-to-FalkorDB/target/release/mariadb_to_falkordb /opt/falkordb/bin/
COPY --from=builder /src/MySQL-to-FalkorDB/target/release/mysql_to_falkordb /opt/falkordb/bin/
COPY --from=builder /src/PostgreSQL-to-FalkorDB/postgres-to-falkordb/target/release/postgres-to-falkordb /opt/falkordb/bin/
COPY --from=builder /src/SQLServer-to-FalkorDB/target/release/sqlserver_to_falkordb /opt/falkordb/bin/
COPY --from=builder /src/Snowflake-to-FalkorDB/target/release/snowflake_to_falkordb /opt/falkordb/bin/
COPY --from=builder /src/Spark-to-FalkorDB/spark-to-falkordb/target/release/spark-to-falkordb /opt/falkordb/bin/

ENV PATH=/opt/falkordb/bin:${PATH}

WORKDIR /workspace
USER runner
CMD ["sleep", "infinity"]
