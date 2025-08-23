# src/etl/load_to_postgres.py
import argparse
import json
import jsonlines
from pathlib import Path
from datetime import datetime
from dateutil import parser as dtparse

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

def _coerce_price_value(x):
    if isinstance(x, dict):
        return x.get("value")
    if isinstance(x, str):
        try:
            d = json.loads(x)
            if isinstance(d, dict):
                return d.get("value")
        except Exception:
            pass
    return np.nan

def _coerce_price_currency(x):
    if isinstance(x, dict):
        return x.get("currency")
    if isinstance(x, str):
        try:
            d = json.loads(x)
            if isinstance(d, dict):
                return d.get("currency")
        except Exception:
            pass
    return None

def _coerce_area_sqm(x):
    if isinstance(x, dict):
        return x.get("sqm")
    if isinstance(x, str):
        try:
            d = json.loads(x)
            if isinstance(d, dict):
                return d.get("sqm")
        except Exception:
            pass
        # simple text fallback like "184 sqm" or "1,200 m²"
        import re
        s = x
        m = re.search(r"(\d+(?:[\.,]\d+)?)\s*(?:m²|m2|sqm|sq\.?\s*m(?:eters?)?)", s, flags=re.I)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except Exception:
                return np.nan
        ft = re.search(r"(\d+(?:[\.,]\d+)?)\s*(?:sq\.?\s*ft|ft²|ft2|square\s*feet)", s, flags=re.I)
        if ft:
            try:
                return round(float(ft.group(1).replace(",", "")) * 0.092903, 2)
            except Exception:
                return np.nan
    return np.nan

def load_jsonl_to_df(path: str) -> pd.DataFrame:
    rows = []
    with jsonlines.open(path) as r:
        for obj in r:
            rows.append(obj)
    df = pd.DataFrame(rows)

    # flatten / normalize
    if "price_value" not in df.columns:
        df["price_value"] = df.get("price", np.nan).apply(_coerce_price_value)
    if "price_currency" not in df.columns:
        df["price_currency"] = df.get("price", None).apply(_coerce_price_currency)

    if "area_sqm" not in df.columns:
        df["area_sqm"] = df.get("area", np.nan).apply(_coerce_area_sqm)

    # friendly aliases
    if "listing_title" not in df.columns and "title" in df.columns:
        df["listing_title"] = df["title"]

    # numeric cleanup
    df["price_value"] = pd.to_numeric(df["price_value"], errors="coerce")
    df["area_sqm"]     = pd.to_numeric(df["area_sqm"], errors="coerce")

    # price_php (assume PHP when price_currency missing or PHP)
    df["price_php"] = np.where(
        (df["price_currency"].isna()) | (df["price_currency"].eq("PHP")),
        df["price_value"],
        np.nan
    )

    # price_per_sqm
    df["price_per_sqm"] = np.where(
        (df["price_php"].notna()) & (df["area_sqm"] > 0),
        df["price_php"] / df["area_sqm"],
        np.nan
    )

    # as_of_date (by day) from scraped_at if present
    if "scraped_at" in df.columns:
        def to_date(s):
            try:
                return dtparse.parse(str(s)).date()
            except Exception:
                return pd.NaT
        df["as_of_date"] = df["scraped_at"].apply(to_date)
    else:
        df["as_of_date"] = pd.to_datetime(datetime.utcnow().date()).date()

    # minimally required columns
    keep = [
        "url","listing_title","price_php","area_sqm","price_per_sqm",
        "address","bedrooms","bathrooms","scraped_at","as_of_date","price_currency","price_value"
    ]
    for k in keep:
        if k not in df.columns:
            df[k] = np.nan
    return df[keep]

def ensure_table(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS listings (
        url TEXT PRIMARY KEY,
        listing_title TEXT,
        price_php DOUBLE PRECISION,
        area_sqm DOUBLE PRECISION,
        price_per_sqm DOUBLE PRECISION,
        address TEXT,
        bedrooms INTEGER,
        bathrooms INTEGER,
        scraped_at TIMESTAMPTZ,
        as_of_date DATE,
        price_currency TEXT,
        price_value DOUBLE PRECISION
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def upsert_df(engine, df: pd.DataFrame, table: str = "listings"):
    # Use INSERT ... ON CONFLICT (url) DO UPDATE
    cols = df.columns.tolist()
    placeholders = ", ".join([f":{c}" for c in cols])
    collist = ", ".join(cols)
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "url"])

    sql = text(f"""
        INSERT INTO {table} ({collist})
        VALUES ({placeholders})
        ON CONFLICT (url) DO UPDATE SET
            {updates}
    """)

    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    chunk = 1000
    with engine.begin() as conn:
        for i in range(0, len(records), chunk):
            conn.execute(sql, records[i:i+chunk])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to <portal>_listings.jsonl")
    ap.add_argument("--table", default="listings")
    ap.add_argument("--database-url", default=None, help="Postgres URL; if omitted, read from env DATABASE_URL")
    args = ap.parse_args()

    dburl = args.database_url or os.getenv("DATABASE_URL")
    if not dburl:
        raise SystemExit("DATABASE_URL not set (env or --database-url).")

    p = Path(args.input)
    if not p.exists():
        raise SystemExit(f"Input not found: {p}")

    df = load_jsonl_to_df(str(p))
    engine = create_engine(dburl)

    ensure_table(engine)
    upsert_df(engine, df, table=args.table)

    print(f"Upserted {len(df)} rows into {args.table}")

if __name__ == "__main__":
    import os
    main()
