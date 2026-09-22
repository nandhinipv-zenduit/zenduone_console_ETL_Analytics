#!/usr/bin/env python3
"""
ZenduOne Billing Report -> Zoho Analytics sync
================================================

Pulls every row from the ZenduOne Admin Console "Billing Report"
(https://zenduone-admin.firebaseapp.com/management/billing/list) and pushes
it into a table in an existing Zoho Analytics workspace (the table itself is
created on first run; the workspace is never created).

--------------------------------------------------------------------------
SOURCE API (reverse-engineered from the admin console's network calls)
--------------------------------------------------------------------------
Base:      https://one-service.zenduit.com
Count:     POST /billing/installations/count   body {}            -> {"count": N}
List:      POST /billing/installations/list    body {offset,limit} -> [ {...row...}, ... ]

Row shape:
    {
      "product": "ZenduCAM",              # Application
      "customer": "PGC",                  # Customer
      "reseller": "DICAN",                # Reseller
      "unitName": "Vehicle",              # Unit
      "totalUnits": 33.3,
      "resellerCost": 100,
      "customerPrice": 100,
      "totalCustomerBilling": 100,
      "totalResellerCost": 33.3
    }

Auth: the console authenticates this API with the `auth_one_console` cookie
value sent verbatim as the `Authorization` header (no "Bearer " prefix,
no separate login endpoint was found on one-service.zenduit.com - this is
a browser-session token, not a service API key). Because of that, this
script expects a *fresh* token to be handed to it via the
ZENDUONE_API_TOKEN environment variable each run - see the
companion scheduled-task setup, which uses Claude in Chrome to read the
cookie from the logged-in admin console session right before invoking
this script.

--------------------------------------------------------------------------
DESTINATION: Zoho Analytics (REST API v2)
--------------------------------------------------------------------------
Uses the org's standard OAuth "self-client" refresh-token flow. It writes
into the EXISTING workspace 953790000013364003
(https://analytics.zoho.com/workspace/953790000013364003) and creates only
the table, on first run, if it isn't there yet.
Every run does a full truncate + reload (the source is a point-in-time
aggregate snapshot, not an append-only log, so there is no natural key to
upsert on) and stamps each row with Synced_At.

Required environment variables:
    ZENDUONE_API_TOKEN            - fresh auth_one_console token (see above)
    zoho_analytics_client_id      - falls back to ZOHO_CLIENT_ID_UNI / ZOHO_CLIENT_ID
    zoho_analytics_client_secret  - falls back to ZOHO_CLIENT_SECRET_UNI / ZOHO_CLIENT_SECRET
    zoho_analytics_refresh_token  - falls back to ZOHO_REFRESH_TOKEN_UNI / ZOHO_REFRESH_TOKEN

Optional:
    ZOHO_ORG_ID                   - default "67409019" (Zenduit org)
    ZOHO_WORKSPACE_ID             - default "953790000013364003"
    ZOHO_TABLE_NAME               - default "Zenduone_console_report"
    ZOHO_ANALYTICS_ACCOUNTS_URL   - default "https://accounts.zoho.com"
    ZOHO_ANALYTICS_API_DOMAIN     - default "https://analyticsapi.zoho.com/restapi/v2"
"""

import os
import sys
import json
import datetime
import requests
import pandas as pd

# ==========================================================
# ZENDUONE BILLING API CONFIG
# ==========================================================
ONE_CONSOLE_BASE = "https://one-service.zenduit.com"
ZENDUONE_API_TOKEN = os.environ.get("ZENDUONE_API_TOKEN")
PAGE_SIZE = int(os.environ.get("ONE_CONSOLE_PAGE_SIZE", "1000"))

# ==========================================================
# ZOHO ANALYTICS CONFIG
# ==========================================================
def _env_first(*names):
    """Return the first non-empty env var among `names` (checked in order)."""
    for name in names:
        val = os.environ.get(name)
        if val:
            return val
    return None


# Prefer zoho_analytics_client_id / zoho_analytics_client_secret /
# zoho_analytics_refresh_token (the names actually set up in this org's
# environment); fall back to the older ZOHO_*_UNI / plain ZOHO_* names in
# case those are what's loaded instead.
ZOHO_ANALYTICS = {
    "client_id": _env_first("zoho_analytics_client_id", "ZOHO_CLIENT_ID_UNI", "ZOHO_CLIENT_ID"),
    "client_secret": _env_first("zoho_analytics_client_secret", "ZOHO_CLIENT_SECRET_UNI", "ZOHO_CLIENT_SECRET"),
    "refresh_token": _env_first("zoho_analytics_refresh_token", "ZOHO_REFRESH_TOKEN_UNI", "ZOHO_REFRESH_TOKEN"),
    # NOTE: deliberately NOT reading a generic "ZOHO_ACCOUNTS_URL" / "ZOHO_API_DOMAIN" —
    # those generic names collide with unrelated Zoho product configs (e.g. Books/CRM
    # use https://www.zohoapis.com, which is NOT the Analytics API host and caused a
    # 404 on /workspaces when that value leaked in here). Analytics needs its own
    # dedicated host, so it gets its own dedicated env var names.
    "accounts_url": os.environ.get("ZOHO_ANALYTICS_ACCOUNTS_URL", "https://accounts.zoho.com"),
    "api_domain": os.environ.get("ZOHO_ANALYTICS_API_DOMAIN", "https://analyticsapi.zoho.com/restapi/v2"),
}
ZOHO_ORG_ID = os.environ.get("ZOHO_ORG_ID", "67409019")

# Target the EXISTING workspace by id — https://analytics.zoho.com/workspace/953790000013364003
# (the same workspace the other Zenduit -> Zoho Analytics sync writes to).
# This script never creates a workspace; only the table is created if missing.
ZOHO_WORKSPACE_ID = os.environ.get("ZOHO_WORKSPACE_ID", "953790000013364003")
ZOHO_TABLE_NAME = os.environ.get("ZOHO_TABLE_NAME", "Zenduone_console_report")
ZOHO_MAX_BYTES_PER_IMPORT = 14 * 1024 * 1024  # stay under the 20MB hard cap

TABLE_COLUMNS = [
    {"COLUMNNAME": "Reseller", "DATATYPE": "PLAIN"},
    {"COLUMNNAME": "Customer", "DATATYPE": "PLAIN"},
    {"COLUMNNAME": "Application", "DATATYPE": "PLAIN"},
    {"COLUMNNAME": "Unit", "DATATYPE": "PLAIN"},
    {"COLUMNNAME": "Total_Units", "DATATYPE": "DECIMAL_NUMBER"},
    {"COLUMNNAME": "Reseller_Cost", "DATATYPE": "DECIMAL_NUMBER"},
    {"COLUMNNAME": "Customer_Price", "DATATYPE": "DECIMAL_NUMBER"},
    {"COLUMNNAME": "Total_Customer_Billing", "DATATYPE": "DECIMAL_NUMBER"},
    {"COLUMNNAME": "Total_Reseller_Cost", "DATATYPE": "DECIMAL_NUMBER"},
    {"COLUMNNAME": "Synced_At", "DATATYPE": "PLAIN"},
]


# ==========================================================
# EXTRACT: ZenduOne Billing Report
# ==========================================================
def _one_console_post(path, payload, timeout=60):
    if not ZENDUONE_API_TOKEN:
        raise SystemExit(
            "ZENDUONE_API_TOKEN is not set. This must be a fresh "
            "'auth_one_console' cookie value copied from an active, logged-in "
            "https://zenduone-admin.firebaseapp.com session (there is no "
            "separate service login endpoint for one-service.zenduit.com)."
        )
    res = requests.post(
        f"{ONE_CONSOLE_BASE}{path}",
        json=payload,
        headers={
            "Authorization": ZENDUONE_API_TOKEN,
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
        },
        timeout=timeout,
    )
    res.raise_for_status()
    return res.json()


def fetch_billing_count():
    data = _one_console_post("/billing/installations/count", {})
    return int(data.get("count", 0))


def fetch_billing_rows():
    total = fetch_billing_count()
    print(f"📊 Billing Report total rows (per API): {total:,}")

    # NOTE: this API does NOT reliably return a full page right up until the
    # last one — it can hand back a short batch (e.g. 999 of 1000 asked for)
    # in the middle of the result set and still have more after it. Stopping
    # on "batch shorter than requested" (the old behavior) silently truncated
    # the pull to ~3,999/8,641 rows in practice. The only trustworthy end
    # condition is an actually EMPTY batch, so we keep going until we see one,
    # with a generous safety cap so a misbehaving API can't loop forever.
    rows = []
    offset = 0
    max_iterations = max(50, (total // max(1, PAGE_SIZE)) * 4 + 20)
    for _ in range(max_iterations):
        batch = _one_console_post(
            "/billing/installations/list",
            {"offset": offset, "limit": PAGE_SIZE},
        )
        if not batch:
            break
        if len(batch) < PAGE_SIZE:
            print(f"   ⚠️  short batch ({len(batch)}/{PAGE_SIZE}) at offset {offset:,} — continuing anyway")
        rows.extend(batch)
        offset += len(batch)
        print(f"   …fetched {offset:,} rows so far")
    else:
        print(f"⚠️  Hit the {max_iterations}-request safety cap while paginating — stopping early.")

    print(f"✔ Total rows fetched: {len(rows):,}")
    if total and len(rows) != total:
        print(
            f"⚠️  Fetched count ({len(rows):,}) differs from reported count "
            f"({total:,}) — the report may have changed while paginating."
        )
    return rows


def build_dataframe(rows):
    df = pd.json_normalize(rows)
    for col in ["product", "customer", "reseller", "unitName", "totalUnits",
                "resellerCost", "customerPrice", "totalCustomerBilling",
                "totalResellerCost"]:
        if col not in df.columns:
            df[col] = None

    out = pd.DataFrame({
        "Reseller": df["reseller"],
        "Customer": df["customer"],
        "Application": df["product"],
        "Unit": df["unitName"],
        "Total_Units": df["totalUnits"],
        "Reseller_Cost": df["resellerCost"],
        "Customer_Price": df["customerPrice"],
        "Total_Customer_Billing": df["totalCustomerBilling"],
        "Total_Reseller_Cost": df["totalResellerCost"],
    })
    out["Synced_At"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    out = out.fillna("")
    return out


# ==========================================================
# ZOHO: AUTH
# ==========================================================
def zoho_get_access_token():
    missing = [k for k in ("client_id", "client_secret", "refresh_token")
               if not ZOHO_ANALYTICS[k]]
    if missing:
        raise SystemExit(
            f"Missing Zoho credential(s): {', '.join(missing)}. Set "
            "zoho_analytics_client_id / zoho_analytics_client_secret / "
            "zoho_analytics_refresh_token (or ZOHO_CLIENT_ID_UNI / "
            "ZOHO_CLIENT_SECRET_UNI / ZOHO_REFRESH_TOKEN_UNI, or the plain "
            "ZOHO_CLIENT_ID / ZOHO_CLIENT_SECRET / ZOHO_REFRESH_TOKEN)."
        )
    r = requests.post(
        f"{ZOHO_ANALYTICS['accounts_url']}/oauth/v2/token",
        data={
            "refresh_token": ZOHO_ANALYTICS["refresh_token"],
            "client_id": ZOHO_ANALYTICS["client_id"],
            "client_secret": ZOHO_ANALYTICS["client_secret"],
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    r.raise_for_status()
    token_data = r.json()
    if "access_token" not in token_data:
        hint = ""
        if token_data.get("error") == "invalid_code":
            hint = (
                " ('invalid_code' from Zoho usually means the refresh token is "
                "expired/revoked/already-used, or it doesn't belong to this "
                "client_id+client_secret pair, or it was issued on a different "
                "Zoho data center than ZOHO_ACCOUNTS_URL points at "
                f"[currently {ZOHO_ANALYTICS['accounts_url']!r}] — "
                "double-check which *_UNI credential set is actually loaded.)"
            )
        raise Exception(f"Failed getting Zoho token: {token_data}{hint}")
    return token_data["access_token"]


def _zoho_headers(access_token):
    return {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": ZOHO_ORG_ID,
    }


# ==========================================================
# ZOHO: TABLE (find-or-create inside the EXISTING workspace)
#
# Workspace creation was removed deliberately — this script writes into the
# already-existing workspace named by ZOHO_WORKSPACE_ID and must never spin
# up a new one.
# ==========================================================
def zoho_find_or_create_table(access_token, workspace_id):
    url = f"{ZOHO_ANALYTICS['api_domain']}/workspaces/{workspace_id}/views"
    r = requests.get(url, headers=_zoho_headers(access_token), timeout=30)
    r.raise_for_status()
    views = r.json().get("data", {}).get("views", [])
    for v in views:
        if v.get("viewName") == ZOHO_TABLE_NAME:
            print(f"✔ Found existing table '{ZOHO_TABLE_NAME}' ({v['viewId']})")
            return v["viewId"]

    print(f"➕ Creating table '{ZOHO_TABLE_NAME}'…")
    create_url = f"{ZOHO_ANALYTICS['api_domain']}/workspaces/{workspace_id}/tables"
    config = {
        "tableDesign": {
            "TABLENAME": ZOHO_TABLE_NAME,
            "TABLEDESCRIPTION": "Synced from ZenduOne Admin Console billing report",
            "COLUMNS": TABLE_COLUMNS,
        }
    }
    r = requests.post(
        create_url,
        headers=_zoho_headers(access_token),
        data={"CONFIG": json.dumps(config)},
        timeout=30,
    )
    r.raise_for_status()
    result = r.json()
    if result.get("status") != "success":
        raise Exception(f"Failed creating table: {result}")
    view_id = result["data"]["viewId"]
    print(f"✔ Created table {view_id}")
    return view_id


# ==========================================================
# ZOHO: IMPORT (chunked truncate + add)
# ==========================================================
def _zoho_import_chunk(workspace_id, view_id, csv_bytes, import_type, access_token):
    url = f"{ZOHO_ANALYTICS['api_domain']}/workspaces/{workspace_id}/views/{view_id}/data"
    config = {
        "importType": import_type,
        "fileType": "csv",
        "autoIdentify": "true",
        "onError": "setcolumnempty",
    }
    files = {"FILE": ("billing_report.csv", csv_bytes, "text/csv")}
    data = {"CONFIG": json.dumps(config)}
    r = requests.post(
        url,
        headers=_zoho_headers(access_token),
        data=data,
        files=files,
        timeout=300,
    )
    print(f"[{import_type}] Status: {r.status_code}")
    if r.status_code != 200:
        print(r.text)
    r.raise_for_status()
    return r.json()


def zoho_truncate_add(df, workspace_id, view_id, access_token):
    header_bytes = len(df.iloc[0:0].to_csv(index=False).encode("utf-8"))
    full_bytes = len(df.to_csv(index=False).encode("utf-8"))
    avg_row = max(1, (full_bytes - header_bytes) // max(1, len(df)))
    rows_per_chunk = max(1, (ZOHO_MAX_BYTES_PER_IMPORT - header_bytes) // avg_row)

    total_rows = len(df)
    print(f"\nUploading {total_rows:,} rows in chunks of {rows_per_chunk:,}")

    for i in range(0, total_rows, rows_per_chunk):
        chunk = df.iloc[i:i + rows_per_chunk]
        csv_bytes = chunk.to_csv(index=False).encode("utf-8")
        import_type = "truncateadd" if i == 0 else "append"
        _zoho_import_chunk(workspace_id, view_id, csv_bytes, import_type, access_token)
        print(f"✔ Uploaded {min(i + len(chunk), total_rows):,}/{total_rows:,}")

    print("✅ Zoho Upload Complete")


# ==========================================================
# MAIN
# ==========================================================
def main():
    print("🔽 Pulling ZenduOne billing report…")
    rows = fetch_billing_rows()
    if not rows:
        raise SystemExit("No rows returned from the billing API — aborting without touching Zoho.")

    df = build_dataframe(rows)
    print(f"🧮 Prepared {len(df):,} rows for Zoho Analytics")

    print("\n🔑 Getting Zoho Analytics access token…")
    access_token = zoho_get_access_token()

    workspace_id = ZOHO_WORKSPACE_ID
    print(f"🗂️  Target workspace: {workspace_id}")
    view_id = zoho_find_or_create_table(access_token, workspace_id)

    zoho_truncate_add(df, workspace_id, view_id, access_token)
    print(f"\n🚀 Synced {len(df):,} billing rows to Zoho Analytics "
          f"(workspace {workspace_id} / '{ZOHO_TABLE_NAME}')")


if __name__ == "__main__":
    main()
