# src/etl/load_to_sqlite.py
import argparse
import glob
import json
import os
import sqlite3
from pathlib import Path

import pandas as pd


def _find_latest_jsonl_for_portal(portal_name: str) -> str | None:
    """
    Finds the most recent listings JSONL for a given portal, searching under scraper_output/run_*/staged.
    """
    pattern = os.path.join("scraper_output", "run_*", "staged", f"{portal_name}_listings.jsonl")
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else None


def _safe_get(d, key, default=None):
    return d.get(key, default) if isinstance(d, dict) else default


def load_and_flatten(jsonl_path: str) -> pd.DataFrame:
    df = pd.read_json(jsonl_path, lines=True)

    # Flatten dicts (safe-get)
    df["price_value"] = df["price"].apply(lambda x: _safe_get(x, "value"))
    df["price_currency"] = df["price"].apply(lambda x: _safe_get(x, "currency"))
    df["price_period"] = df["price"].apply(lambda x: _safe_get(x, "period"))
    df["price_raw"] = df["price"].apply(lambda x: _safe_get(x, "raw"))

    df["area_sqm"] = df["area"].apply(lambda x: _safe_get(x, "sqm"))
    df["area_raw"] = df["area"].apply(lambda x: _safe_get(x, "raw"))

    # ✅ Drop original nested columns to avoid dicts going into SQLite
    df = df.drop(columns=["price", "area"], errors="ignore")

    # Types
    for col in ["price_value", "area_sqm"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["bedrooms", "bathrooms"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Derived metric
    df["price_per_sqm"] = df.apply(
        lambda r: (r["price_value"] / r["area_sqm"])
        if pd.notnull(r["price_value"]) and pd.notnull(r["area_sqm"]) and r["area_sqm"] > 0
        else pd.NA,
        axis=1,
    )

    # De-dup by URL (keep latest)
    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
        df.sort_values(["url", "scraped_at"], inplace=True)
        df = df.drop_duplicates(subset=["url"], keep="last")
        # Store as ISO text for SQLite
        df["scraped_at"] = df["scraped_at"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        df = df.drop_duplicates(subset=["url"], keep="last")

    # Order columns
    preferred = [
        "url", "title", "address",
        "price_value", "price_currency", "price_period", "price_raw",
        "area_sqm", "area_raw",
        "price_per_sqm",
        "bedrooms", "bathrooms",
        "description",
        "scraped_at",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    return df



def save_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "listings", if_exists: str = "replace"):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)

        # Helpful indexes for speed
        try:
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_url ON {table_name}(url);')
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_addr ON {table_name}(address);')
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_price ON {table_name}(price_value);')
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_area ON {table_name}(area_sqm);')
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_pps ON {table_name}(price_per_sqm);')
        except Exception:
            # Non-fatal if index creation fails
            pass


def main():
    ap = argparse.ArgumentParser(description="ETL: Load scraped JSONL -> clean -> SQLite")
    ap.add_argument("--input", help="Path to listings.jsonl (if omitted, use --portal to auto-find latest)")
    ap.add_argument("--portal", help="Portal name to auto-find the latest JSONL under scraper_output/run_*/staged")
    ap.add_argument("--db", default="data/db/central.db", help="SQLite DB path (default: data/db/central.db)")
    ap.add_argument("--table", default="listings", help="Table name (default: listings)")
    ap.add_argument("--if-exists", default="replace", choices=["fail", "replace", "append"],
                    help="Behavior if table exists (default: replace)")
    args = ap.parse_args()

    jsonl_path = args.input
    if not jsonl_path:
        if not args.portal:
            raise SystemExit("Provide --input path or --portal name to locate latest JSONL automatically.")
        jsonl_path = _find_latest_jsonl_for_portal(args.portal)
        if not jsonl_path:
            raise SystemExit(f"No JSONL found for portal '{args.portal}' under scraper_output/run_*/staged.")

    print(f"Loading: {jsonl_path}")
    df = load_and_flatten(jsonl_path)
    print(f"Rows: {len(df)} | Columns: {len(df.columns)}")

    print(f"Saving to SQLite: {args.db} (table: {args.table}, if_exists={args.if_exists})")
    save_to_sqlite(df, args.db, args.table, args.if_exists)
    print("Done.")


if __name__ == "__main__":
    main()
