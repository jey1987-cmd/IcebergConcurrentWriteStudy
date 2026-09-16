#!/usr/bin/env python3
"""
Custom PyIceberg AuthManager that authenticates via the gcloud CLI's own
OAuth client (the identity behind `gcloud auth login`), rather than
Application Default Credentials.

Why this exists: BigLake's Iceberg REST catalog endpoint rejects access
tokens minted by ADC's well-known OAuth client (401
ACCESS_TOKEN_TYPE_UNSUPPORTED) -- confirmed by direct testing against the
endpoint. Tokens from `gcloud auth print-access-token` (the gcloud CLI's
own client) are accepted. A service account key was the standard
alternative, but this project enforces the
iam.disableServiceAccountKeyCreation org policy, so no key can be minted
at all.

Requires the gcloud CLI to be installed and authenticated
(`gcloud auth login`) on whatever machine imports this.
"""

import subprocess

from pyiceberg.catalog.rest.auth import AuthManager

GCLOUD_PATH = r"C:\Users\jey19\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"


class GcloudCliAuthManager(AuthManager):
    def auth_header(self) -> str:
        token = subprocess.check_output(
            [GCLOUD_PATH, "auth", "print-access-token"],
            text=True,
        ).strip()
        return f"Bearer {token}"
