#!/usr/bin/env python3
"""
Shared BigLake Iceberg REST catalog connection helper.

Every script that touches the shared table (baseline writers, agent
scripts, analysis) should import get_catalog() from here rather than
duplicating the connection properties.
"""

import json

import gcloud_auth_manager  # noqa: F401  -- registers via auth.impl below
from pyiceberg.catalog import load_catalog

with open("gcp_profile.json") as f:
    _profile = json.load(f)

PROJECT_ID = _profile["project_id"]
CATALOG_ID = _profile["iceberg_catalog_id"]

NAMESPACE = "concurrent_agents"
TABLE_NAME = "concurrent_writes"
TABLE_IDENTIFIER = f"{NAMESPACE}.{TABLE_NAME}"


def get_catalog():
    return load_catalog(
        "biglake",
        **{
            "type": "rest",
            "uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            "warehouse": f"bl://projects/{PROJECT_ID}/catalogs/{CATALOG_ID}",
            "auth": {
                "type": "custom",
                "impl": "gcloud_auth_manager.GcloudCliAuthManager",
            },
            "header.x-goog-user-project": PROJECT_ID,
            # Catalog was created with credential-mode=end-user.
            "header.X-Iceberg-Access-Delegation": "",
        },
    )


def get_row_count(table) -> int:
    """Cheap: reads total-records from the current snapshot's summary
    metadata rather than scanning data. 0 for an empty/new table.
    Used to track and control for table-growth effects across runs --
    confirmed directly that quarantine_writer's scan-based target-picking
    cost climbs with table size over the course of a run."""
    snapshot = table.current_snapshot()
    if snapshot is None:
        return 0
    return int(snapshot.summary.get("total-records", 0))
