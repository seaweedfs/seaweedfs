"""Round-trip a Delta table through Unity Catalog using delta-rs.

Reads connection settings from environment variables:

    UC_URL          Unity Catalog REST base, e.g. http://host.docker.internal:8080
    S3_ENDPOINT     SeaweedFS S3 endpoint reachable from the container
    UC_CATALOG      Catalog name to operate against
    UC_SCHEMA       Schema name
    UC_TABLE        Table name
    UC_TABLE_ID     Optional: pre-fetched table_id; falls back to GET /tables/{full_name}

Steps:

    1. GET the table info (if UC_TABLE_ID is unset).
    2. POST /temporary-table-credentials with operation=READ_WRITE.
    3. write_deltalake() a small pyarrow Table to storage_location.
    4. Read it back with DeltaTable() and assert row count.

Prints `DELTA_RS_OK rows=<n>` on success and exits 0; any failure exits 1.
"""

from __future__ import annotations

import os
import sys
import urllib.parse

import pyarrow as pa
import requests
from deltalake import DeltaTable, write_deltalake


def must_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: required env var {name} is not set", file=sys.stderr)
        sys.exit(2)
    return value


def main() -> int:
    uc_url = must_env("UC_URL").rstrip("/")
    s3_endpoint = must_env("S3_ENDPOINT")
    catalog = must_env("UC_CATALOG")
    schema = must_env("UC_SCHEMA")
    table = must_env("UC_TABLE")
    table_id = os.environ.get("UC_TABLE_ID", "").strip()

    full_name = f"{catalog}.{schema}.{table}"
    api = f"{uc_url}/api/2.1/unity-catalog"

    if not table_id:
        r = requests.get(f"{api}/tables/{urllib.parse.quote(full_name)}", timeout=10)
        r.raise_for_status()
        info = r.json()
        table_id = info.get("table_id") or ""
        storage_location = info.get("storage_location") or ""
    else:
        r = requests.get(f"{api}/tables/{urllib.parse.quote(full_name)}", timeout=10)
        r.raise_for_status()
        storage_location = r.json().get("storage_location") or ""

    if not table_id:
        print("ERROR: table_id is empty in UC response", file=sys.stderr)
        return 1
    if not storage_location:
        print("ERROR: storage_location is empty in UC response", file=sys.stderr)
        return 1

    creds_resp = requests.post(
        f"{api}/temporary-table-credentials",
        json={"table_id": table_id, "operation": "READ_WRITE"},
        timeout=15,
    )
    creds_resp.raise_for_status()
    creds = creds_resp.json().get("aws_temp_credentials") or {}
    if not creds.get("access_key_id") or not creds.get("secret_access_key"):
        print(f"ERROR: missing aws_temp_credentials in {creds_resp.json()}", file=sys.stderr)
        return 1

    storage_options = {
        "AWS_ACCESS_KEY_ID": creds["access_key_id"],
        "AWS_SECRET_ACCESS_KEY": creds["secret_access_key"],
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT_URL": s3_endpoint,
        "AWS_ALLOW_HTTP": "true",
        # delta-rs requires this for non-S3 backends that lack atomic rename.
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    if creds.get("session_token"):
        storage_options["AWS_SESSION_TOKEN"] = creds["session_token"]

    arrow_table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value": pa.array(["alpha", "beta", "gamma"], type=pa.string()),
        }
    )

    write_deltalake(
        storage_location,
        arrow_table,
        storage_options=storage_options,
        mode="overwrite",
    )

    dt = DeltaTable(storage_location, storage_options=storage_options)
    read_back = dt.to_pyarrow_table()
    rows = read_back.num_rows
    print(f"DELTA_RS_OK rows={rows} version={dt.version()}")
    return 0 if rows == arrow_table.num_rows else 1


if __name__ == "__main__":
    sys.exit(main())
