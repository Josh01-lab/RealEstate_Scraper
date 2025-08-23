# src/etl/push_to_supabase.py
import argparse
import os
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

import jsonlines
import pandas as pd
import numpy as np

from src.db.supabase_io import upsert_rows

def _coerce_jsonish(x):
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        s = x.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                pass
    return None

def _extract_price_php(rec: Dict[str, Any]) -> Optional[float]:
    # preferred shape: rec["price"] is a dict with .value and .currency
    p = rec.get("price")
    if p is not None:
        p = _coerce_jsonish(p)
    if isinstance(p, dict):
        cur = (p.get("currency") or "PHP").upper()
        val = p.get("value")
        if cur == "PHP" and isinstance(val, (int, float)):
            return float(val)
    # fallback explicit
    if isinstance(rec.get("price_php"), (int, float)):
        return float(rec["price_php"])
    # last resort: parse "₱ 123,456"
    if isinstance(rec.get("price"), str):
        import re
        digits = re.sub(r"[^\d.]", "", rec["price"].replace(",", ""))
        try:
            return float(digits) if digits else None
        except Exception:
            return None
    return None

def _extract_area_sqm(rec: Dict[str, Any]) -> Optional[float]:
    a = rec.get("area")
    if a is not None:
        a = _coerce_jsonish(a)
    if isinstance(a, dict) and a.get("sqm") is not None:
        try:
            return float(a["sqm"])
        except Exception:
            return None
    if isinstance(rec.get("area_sqm"), (int, float, str)):
        try:
            return float(rec["area_sqm"])
        except Exception:
            return None
    # fallback regex from description
    desc = rec.get("description") or ""
    if isinstance(desc, str):
        import re
        m = re.search(r"(\d+(?:[\.,]\d+)?)\s*(sqm|m²|m2|sq\.?\s*m)", desc, flags=re.I)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except Exception:
                return None
    return None

def _to_supabase_row(rec: Dict[str, Any]) -> Dict[str, Any]:
    price_php = _extract_price_php(rec)
    area_sqm  = _extract_area_sqm(rec)
    ppsqm = None
    if isinstance(price_php, (int, float)) and isinstance(area_sqm, (int, float)) and area_sqm > 0:
        ppsqm = price_php / area_sqm

    return {
        "url": rec.get("url"),
        "listing_title": rec.get("title"),
        "property_type": rec.get("property_type"),
        "address": rec.get("address"),
        "price_php": price_php,
        "area_sqm": area_sqm,
        "price_per_sqm": ppsqm,
        "price_json": rec.get("price"),
        "area_json": rec.get("area"),
        "scraped_at": rec.get("scraped_at"),
        "source": rec.get("source") or "lamudi_cebu",
    }

def _find_default_jsonl(portal: str) -> Path:
    # Pick the newest run_* dir and return that portal's listings file
    base = Path("scraper_output")
    runs = sorted(base.glob("run_*"), reverse=True)
    for r in runs:
        p = r / "staged" / f"{portal}_listings.jsonl"
        if p.exists():
            return p
    raise FileNotFoundError(f"No listings file found for portal '{portal}' under scraper_output/run_*/staged/")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="Path to *listings.jsonl (if omitted, auto-pick latest run for --portal)")
    ap.add_argument("--portal", default="lamudi_cebu", help="Portal name (used if --input is omitted)")
    ap.add_argument("--table", default="listings", help="Supabase table name")
    ap.add_argument("--conflict", default="url", help="Upsert conflict target (unique indexed column)")
    ap.add_argument("--min_area", type=float, default=1.0, help="Discard rows with area_sqm < min_area")
    args = ap.parse_args()

    # Resolve input file
    in_path = Path(args.input) if args.input else _find_default_jsonl(args.portal)
    if not in_path.exists():
        raise FileNotFoundError(f"Input JSONL not found: {in_path}")

    # Load JSONL -> list[dict]
    records: List[Dict[str, Any]] = []
    with jsonlines.open(in_path, "r") as reader:
        for rec in reader:
            if not isinstance(rec, dict):
                continue
            records.append(rec)

    # Map & basic QC
    rows = []
    for rec in records:
        row = _to_supabase_row(rec)
        # Basic quality gate: needs url, price, area > threshold
        if not row.get("url"):
            continue
        if row.get("area_sqm") is None or row["area_sqm"] < args.min_area:
            continue
        if row.get("price_php") is None or row["price_php"] <= 0:
            continue
        rows.append(row)

    if not rows:
        print("No valid rows to push (after QC).")
        return

    # Upsert
    upsert_rows(rows, table=args.table, conflict=args.conflict)
    print(f"Pushed {len(rows)} row(s) to Supabase table '{args.table}'.")

if __name__ == "__main__":
    main()
