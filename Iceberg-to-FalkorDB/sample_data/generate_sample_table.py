#!/usr/bin/env python3
"""Generate a local Iceberg warehouse for the Iceberg-to-FalkorDB demo.

Creates a SQL/sqlite catalog + local `file://` warehouse under this
script's own directory, seeded with `sales.customers` and `sales.orders`
tables that match `../iceberg_sample_to_falkordb.yaml`. Also performs an
append + a row-level delete on `sales.orders` so a full `table.scan()`
demonstrates Iceberg's merge-on-read delete handling.

The generated `catalog.db` / `warehouse/` are NOT committed to git (they
embed machine-specific absolute paths); run this script to (re)create
them locally. Safe to re-run: it wipes and rebuilds from scratch.

Usage:
    pip install -r requirements.txt
    eval "$(python3 generate_sample_table.py)"

On success, prints two `export` lines (and only those lines) to stdout so
the output can be consumed directly via `eval "$(...)"`. All progress/log
messages go to stderr.
"""
import datetime
import shutil
import sys
from pathlib import Path

import pyarrow as pa

BASE_DIR = Path(__file__).resolve().parent
WAREHOUSE_DIR = BASE_DIR / "warehouse"
CATALOG_DB = BASE_DIR / "catalog.db"

# The Rust connector opens the SQL catalog via `SqlCatalogBuilder::load("sql", ...)`;
# iceberg-catalog-sql scopes table rows by this catalog name, so it must match
# exactly or the connector will report `TableNotFound` even though the table exists.
CATALOG_NAME = "sql"


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


def main() -> None:
    from pyiceberg.catalog.sql import SqlCatalog

    if WAREHOUSE_DIR.exists():
        shutil.rmtree(WAREHOUSE_DIR)
    CATALOG_DB.unlink(missing_ok=True)
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    log(f"Creating local Iceberg catalog at {CATALOG_DB}")
    log(f"Using local warehouse at {WAREHOUSE_DIR}")

    catalog = SqlCatalog(
        CATALOG_NAME,
        **{
            "uri": f"sqlite:///{CATALOG_DB}",
            "warehouse": f"file://{WAREHOUSE_DIR}",
        },
    )
    catalog.create_namespace_if_not_exists("sales")

    t0 = datetime.datetime(2024, 1, 1)
    t1 = datetime.datetime(2024, 1, 2)

    customers_schema = pa.schema(
        [
            ("customer_id", pa.int64()),
            ("email", pa.string()),
            ("name", pa.string()),
            ("updated_at", pa.timestamp("us")),
            ("is_deleted", pa.bool_()),
        ]
    )
    customers_table = catalog.create_table("sales.customers", schema=customers_schema)
    customers_table.append(
        pa.table(
            {
                "customer_id": [1, 2, 3],
                "email": ["alice@example.com", "bob@example.com", "carol@example.com"],
                "name": ["Alice", "Bob", "Carol"],
                "updated_at": pa.array([t0, t0, t0], type=pa.timestamp("us")),
                "is_deleted": [False, False, False],
            },
            schema=customers_schema,
        )
    )
    log("Seeded sales.customers (3 rows)")

    orders_schema = pa.schema(
        [
            ("order_id", pa.int64()),
            ("customer_id", pa.int64()),
            ("status", pa.string()),
            ("total", pa.float64()),
            ("ordered_at", pa.timestamp("us")),
            ("updated_at", pa.timestamp("us")),
            ("is_deleted", pa.bool_()),
        ]
    )
    orders_table = catalog.create_table("sales.orders", schema=orders_schema)
    orders_table.append(
        pa.table(
            {
                "order_id": [101, 102, 103],
                "customer_id": [1, 2, 1],
                "status": ["shipped", "pending", "shipped"],
                "total": [25.50, 12.00, 42.75],
                "ordered_at": pa.array([t0, t0, t0], type=pa.timestamp("us")),
                "updated_at": pa.array([t0, t0, t0], type=pa.timestamp("us")),
                "is_deleted": [False, False, False],
            },
            schema=orders_schema,
        )
    )
    # A second snapshot (later watermark) plus a merge-on-read delete, so a
    # full `table.scan()` demonstrates both incremental filtering and delete
    # handling, matching the connector's documented v1 design (ADR-0001).
    orders_table.append(
        pa.table(
            {
                "order_id": [104],
                "customer_id": [3],
                "status": ["pending"],
                "total": [99.99],
                "ordered_at": pa.array([t1], type=pa.timestamp("us")),
                "updated_at": pa.array([t1], type=pa.timestamp("us")),
                "is_deleted": [False],
            },
            schema=orders_schema,
        )
    )
    orders_table.delete(delete_filter="order_id == 102")
    log("Seeded sales.orders (4 rows appended, 1 deleted via merge-on-read)")

    catalog_uri = f"sqlite:///{CATALOG_DB}"
    warehouse_uri = f"file://{WAREHOUSE_DIR}"
    log("Done. Export these before running the connector:")
    print(f'export ICEBERG_SAMPLE_CATALOG_URI="{catalog_uri}"')
    print(f'export ICEBERG_SAMPLE_WAREHOUSE="{warehouse_uri}"')


if __name__ == "__main__":
    main()
