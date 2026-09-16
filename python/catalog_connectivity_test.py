#!/usr/bin/env python3
"""
PyIceberg <-> BigLake Iceberg REST catalog connectivity check.

Confirms the catalog connection, namespace creation, table creation, and a
basic write/read round-trip all work end to end -- the checkpoint to clear
before any agent logic gets written on top of this.

Requires: pip install "pyiceberg[gcsfs]" pyarrow
Auth: uses gcloud_auth_manager.GcloudCliAuthManager (see that file for why)
-- BigLake's Iceberg REST catalog endpoint rejects both ADC user tokens
and service-account tokens minted the normal way for this project (org
policy blocks SA key creation entirely), so this reuses the gcloud CLI's
own OAuth client, which the endpoint does accept.
"""

import datetime

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from catalog_client import get_catalog

NAMESPACE = "concurrent_agents"
TABLE_NAME = "connectivity_check"


def main():
    catalog = get_catalog()

    if (NAMESPACE,) not in catalog.list_namespaces():
        catalog.create_namespace(NAMESPACE)
        print(f"Created namespace: {NAMESPACE}")
    else:
        print(f"Namespace already exists: {NAMESPACE}")

    table_identifier = f"{NAMESPACE}.{TABLE_NAME}"
    if catalog.table_exists(table_identifier):
        table = catalog.load_table(table_identifier)
        print(f"Loaded existing table: {table_identifier}")
    else:
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "message", StringType(), required=False),
            NestedField(3, "written_at", TimestampType(), required=False),
        )
        table = catalog.create_table(table_identifier, schema=schema)
        print(f"Created table: {table_identifier}")

    written_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    arrow_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("message", pa.string(), nullable=True),
        pa.field("written_at", pa.timestamp("us"), nullable=True),
    ])
    data = pa.table(
        {
            "id": [1],
            "message": ["connectivity check"],
            "written_at": [written_at],
        },
        schema=arrow_schema,
    )
    table.append(data)
    print("Appended 1 row.")

    result = table.scan().to_arrow()
    print(f"Read back {result.num_rows} row(s) total:")
    print(result.to_pylist())


if __name__ == "__main__":
    main()
