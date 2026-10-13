#!/usr/bin/env python3
"""
ZenduOne Billing Report -> Zoho Analytics sync  (v2 - pagination fix)
=====================================================================

Pulls every row from the ZenduOne Admin Console "Billing Report"
(https://zenduone-admin.firebaseapp.com/management/billing/list) and pushes
it into a table in an existing Zoho Analytics workspace (the table is created
on first run; the workspace is never created).

WHAT CHANGED IN v2
------------------
Many customers (e.g. Absolute Interior) were missing from Zoho even though the
row count looked right. The likely cause: /billing/installations/list is an
aggregated report with no stable sort, so offset/limit paging returns
overlapping pages - some rows come back twice, others never. v2:

  1. Tries to fetch the whole report in ONE request (limit = count + buffer),
     which avoids paging entirely.
  2. If the API caps the page size, falls back to paging, but de-duplicates
     rows and re-runs the paging pass (up to MAX_PASSES) until it has as many
     unique rows as the API's count says exist.
  3. Refuses to touch Zoho if it still has fewer unique rows than the count
     (so a partial pull never wipes good data with a truncate). Override with
     ALLOW_PARTIAL=1.
  4. Prints a debug line for DEBUG_CUSTOMER (default "absolute interior") so
     you can see whether that customer was fetched.

SOURCE API (reverse-engineered from the admin console's network calls)
----------------------------------------------------------------------
Base:  https://one-service.zenduit.com
Count: POST /billing/installations/count  body {}              -> {"count": N}
List:  POST /billing/installations/list   body {offset, limit} -> [ {...}, ... ]

Auth: the `auth_one_console` cookie value from a logged-in admin console
session, sent verbatim as the `Authorization` header. Pass it in via
ZENDUONE_API_TOKEN each run.

Required environment variables:
    ZENDUONE_API_TOKEN            - fresh auth_one_console token
    zoho_analytics_client_id      - falls back to ZOHO_CLIENT_ID_UNI / ZOHO_CLIENT_ID
    zoho_analytics_client_secret  - falls back to ZOHO_CLIENT_SECRET_UNI / ZOHO_CLIENT_SECRET
    zoho_analytics_refresh_token  - falls back to ZOHO_REFRESH_TOKEN_UNI / ZOHO_REFRESH_TOKEN

Optional:
    ZOHO_ORG_ID                   - default "67409019"
    ZOHO_WORKSPACE_ID             - default "953790000013364003"
    ZOHO_TABLE_NAME               - default "Zenduone_console_report"
    ZOHO_ANALYTICS_ACCOUNTS_URL   - default "https://accounts.zoho.com"
    ZOHO_ANALYTICS_API_DOMAIN     - default "https://analyticsapi.zoho.com/restapi/v2"
    ONE_CONSOLE_PAGE_SIZE         - fallback page size, default 1000
    ONE_CONSOLE_MAX_PASSES        - fallback paging passes, default 5
    ALLOW_PARTIAL                 - "1" to upload even if rows are missing
    DEBUG_CUSTOMER                - customer name to trace, default "absolute interior"
    DRY_RUN                       - "1" to fetch + validate only, no Zoho upload
"""

import os
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
MAX_PASSES = int(os.environ.get("ONE_CONSOLE_MAX_PASSES", "5"))
ALLOW_PARTIAL = os.environ.get("ALLOW_PARTIAL") == "1"
DRY_RUN = os.environ.get("DRY_RUN") == "1"
DEBUG_CUSTOMER = os.environ.get("DEBUG_CUSTOMER", "absolute interior").strip().lower()

ROW_FIELDS = ["product", "customer", "reseller", "unitName", "totalUnits",
              "resellerCost", "customerPrice", "totalCustomerBilling",
              "totalResellerCost"]


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


ZOHO_ANALYTICS = {
    "client_id": _env_first("zoho_analytics_client_id", "ZOHO_CLIENT_ID_UNI", "ZOHO_CLIENT_ID"),
    "client_secret": _env_first("zoho_analytics_client_secret", "ZOHO_CLIENT_SECRET_UNI", "ZOHO_CLIENT_SECRET"),
    "refresh_token": _env_first("zoho_analytics_refresh_token", "ZOHO_REFRESH_TOKEN_UNI", "ZOHO_REFRESH_TOKEN"),
    # Dedicated Analytics env names on purpose - generic ZOHO_API_DOMAIN values
    # from Books/CRM point at the wrong host and cause 404s.
    "accounts_url": os.environ.get("ZOHO_ANALYTICS_ACCOUNTS_URL", "https://accounts.zoho.com"),
    "api_domain": os.environ.get("ZOHO_ANALYTICS_API_DOMAIN", "https://analyticsapi.zoho.com/restapi/v2"),
}
ZOHO_ORG_ID = os.environ.get("ZOHO_ORG_ID", "67409019")
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
def _one_console_post(path, payload, timeout=120):
    if not ZENDUONE_API_TOKEN:
        raise SystemExit(
            "ZENDUONE_API_TOKEN is not set. It must be a fresh 'auth_one_console' "
            "cookie value from a logged-in https://zenduone-admin.firebaseapp.com session."
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
    if res.status_code in (401, 403):
        raise SystemExit(
            f"ZenduOne API returned {res.status_code} - the auth_one_console token "
            "is expired or invalid. Grab a fresh one from the console and re-run."
        )
    res.raise_for_status()
    return res.json()


def fetch_billing_count():
    data = _one_console_post("/billing/installations/count", {})
    return int(data.get("count", 0))


def _row_key(row):
    """Identity of a row = all its billing fields. Exact repeats are paging duplicates."""
    return tuple(json.dumps(row.get(f), sort_keys=True, default=str) for f in ROW_FIELDS)


def _add_unique(rows, store):
    """Add rows to `store` (dict key -> row). Returns how many were new."""
    new = 0
    for r in rows:
        k = _row_key(r)
        if k not in store:
            store[k] = r
            new += 1
    return new


def _fetch_single_shot(total):
    """Try to get the whole report in one request (no paging = no paging bugs)."""
    limit = total + 1000
    print(f"   Trying single request with limit={limit:,}…")
    try:
        batch = _one_console_post(
            "/billing/installations/list", {"offset": 0, "limit": limit}, timeout=300
        )
    except requests.HTTPError as e:
        print(f"   Single request rejected ({e}) - falling back to paging.")
        return []
    print(f"   Single request returned {len(batch):,} rows")
    return batch or []


def _fetch_paged_pass(store, pass_no):
    """One full offset/limit walk. Stops only on an EMPTY batch."""
    offset = 0
    fetched = 0
    new_total = 0
    max_iterations = 500
    for _ in range(max_iterations):
        batch = _one_console_post(
            "/billing/installations/list", {"offset": offset, "limit": PAGE_SIZE}
        )
        if not batch:
            break
        new = _add_unique(batch, store)
        new_total += new
        fetched += len(batch)
        dupes = len(batch) - new
        print(f"   pass {pass_no} offset {offset:,}: got {len(batch):,}, "
              f"new {new:,}, already-seen {dupes:,}  (unique so far {len(store):,})")
        offset += len(batch)
    else:
        print(f"⚠️  Hit the {max_iterations}-request safety cap on pass {pass_no}.")
    return fetched, new_total


def fetch_billing_rows():
    total = fetch_billing_count()
    print(f"📊 Billing Report total rows (per API count): {total:,}")

    store = {}

    # 1) Single request - best case, avoids unstable paging entirely.
    single = _fetch_single_shot(total)
    _add_unique(single, store)
    print(f"   Unique rows after single request: {len(store):,}/{total:,}")

    # 2) Fallback: repeated paging passes, de-duplicated, until we reach `total`
    #    or two passes in a row add nothing new.
    pass_no = 0
    empty_passes = 0
    while len(store) < total and pass_no < MAX_PASSES:
        pass_no += 1
        print(f"🔁 Paging pass {pass_no}/{MAX_PASSES} (page size {PAGE_SIZE:,})…")
        fetched, new = _fetch_paged_pass(store, pass_no)
        print(f"   pass {pass_no} done: fetched {fetched:,}, new unique {new:,}, "
              f"unique total {len(store):,}/{total:,}")
        empty_passes = empty_passes + 1 if new == 0 else 0
        if empty_passes >= 2:
            print("   Two passes in a row added nothing new - stopping.")
            break

    rows = list(store.values())
    print(f"✔ Unique rows collected: {len(rows):,} (API count {total:,})")

    # Trace a specific customer so you can see whether it came through.
    if DEBUG_CUSTOMER:
        hits = [r for r in rows if DEBUG_CUSTOMER in str(r.get("customer", "")).lower()]
        print(f"🔎 Rows matching customer '{DEBUG_CUSTOMER}': {len(hits)}")
        for r in hits[:20]:
            print(f"     {r}")

    return rows, total


def build_dataframe(rows):
    df = pd.json_normalize(rows)
    for col in ROW_FIELDS:
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
    out = out.sort_values(["Reseller", "Customer", "Application", "Unit"], na_position="last")
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
            "zoho_analytics_refresh_token (or the *_UNI / plain ZOHO_* names)."
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
                " ('invalid_code' usually means the refresh token is expired/revoked, "
                "doesn't belong to this client_id+secret, or was issued on a different "
                f"Zoho data center than {ZOHO_ANALYTICS['accounts_url']!r}.)"
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
# ZOHO: IMPORT (chunked truncate + append)
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
    result = r.json()
    summary = result.get("data", {}).get("importSummary") or result.get("data", {})
    print(f"   Zoho import summary: {json.dumps(summary)[:500]}")
    return result


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
    rows, total = fetch_billing_rows()
    if not rows:
        raise SystemExit("No rows returned from the billing API — aborting without touching Zoho.")

    # Safety gate: never truncate Zoho with a partial pull.
    if total and len(rows) < total:
        msg = (f"Only {len(rows):,} unique rows collected but the API count is {total:,} "
               f"({total - len(rows):,} missing).")
        if not ALLOW_PARTIAL:
            raise SystemExit(f"❌ {msg} Aborting without touching Zoho. "
                             "Set ALLOW_PARTIAL=1 to upload anyway.")
        print(f"⚠️  {msg} Continuing because ALLOW_PARTIAL=1.")
    elif total and len(rows) > total:
        print(f"ℹ️  Collected {len(rows):,} unique rows, more than the API count {total:,} "
              "(report probably changed during the pull).")

    df = build_dataframe(rows)
    print(f"🧮 Prepared {len(df):,} rows for Zoho Analytics "
          f"({df['Customer'].nunique():,} distinct customers)")

    if DRY_RUN:
        out_csv = "billing_report_dry_run.csv"
        df.to_csv(out_csv, index=False)
        print(f"🧪 DRY_RUN=1 - wrote {out_csv}, Zoho not touched.")
        return

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
