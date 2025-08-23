# src/dashboard/app.py
import argparse
import sqlite3
from pathlib import Path
import json as _json
import re

import numpy as np
import pandas as pd
import streamlit as st


# ---------- config ----------
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--db", default="data/db/central.db")  # <- change if needed
args, _ = parser.parse_known_args()
DB_PATH = args.db

st.set_page_config(page_title="Cebu Commercial Rental Market Intelligence", layout="wide")


# ---------- helpers ----------
def _coerce_jsonish(x):
    """Return dict if x is a dict or JSON-string; else None."""
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        s = x.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return _json.loads(s)
            except Exception:
                pass
    return None


@st.cache_data(show_spinner=False)
def load_data(db_path: str, table: str = "listings") -> pd.DataFrame:
    if not Path(db_path).exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    with sqlite3.connect(db_path) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)

    # ---------- PRICE ----------
    if "price_php" not in df.columns:
        price_val = None
        if {"price_value", "price_currency"}.issubset(df.columns):
            price_val = np.where(df["price_currency"].eq("PHP"), df["price_value"], np.nan)
        elif "price" in df.columns:
            p = df["price"].apply(_coerce_jsonish)
            price_val = p.apply(
                lambda d: d.get("value") if isinstance(d, dict) and d.get("currency") in (None, "PHP") else np.nan
            )
            mask_text = p.isna() & df["price"].notna()
            if mask_text.any():
                def _num_from_text(s):
                    try:
                        digits = re.sub(r"[^\d.]", "", str(s).replace(",", ""))
                        return float(digits) if digits else np.nan
                    except Exception:
                        return np.nan
                price_val = pd.Series(price_val, copy=True)
                price_val.loc[mask_text] = df.loc[mask_text, "price"].apply(_num_from_text)
        elif "price_value" in df.columns:
            price_val = df["price_value"]

        df["price_php"] = pd.to_numeric(price_val, errors="coerce") if price_val is not None else np.nan

    # ---------- TITLE ----------
    if "listing_title" not in df.columns:
        df["listing_title"] = df["title"] if "title" in df.columns else ""

    # ---------- AREA ----------
    if "area_sqm" not in df.columns:
        area = pd.Series(np.nan, index=df.index, dtype="float64")
        if "area" in df.columns:
            ajson = df["area"].apply(_coerce_jsonish)
            has_dict = ajson.apply(lambda d: isinstance(d, dict) and d.get("sqm") is not None)
            area.loc[has_dict] = pd.to_numeric(ajson[has_dict].apply(lambda d: d.get("sqm")), errors="coerce")
            mask_text = (~has_dict) & df["area"].notna()
            if mask_text.any():
                def _sqm_from_text(s):
                    s = str(s)
                    m = re.search(r"(\d+(?:[\.,]\d+)?)\s*(?:m²|m2|sqm|sq\.?\s*m(?:eters?)?)", s, flags=re.I)
                    if m:
                        try: return float(m.group(1).replace(",", ""))
                        except Exception: return np.nan
                    ft = re.search(r"(\d+(?:[\.,]\d+)?)\s*(?:sq\.?\s*ft|ft²|ft2|square\s*feet)", s, flags=re.I)
                    if ft:
                        try: return round(float(ft.group(1).replace(",", "")) * 0.092903, 2)
                        except Exception: return np.nan
                    return np.nan
                area.loc[mask_text] = pd.to_numeric(df.loc[mask_text, "area"].apply(_sqm_from_text), errors="coerce")

        if area.isna().all() and "description" in df.columns:
            area_from_desc = df["description"].apply(
                lambda s: np.nan if pd.isna(s) else
                (lambda m: float(m.group(1)) if m else np.nan)(
                    re.search(r"(\d+(?:\.\d+)?)\s*(?:sqm|m²|m2|sq\.?\s*m)", str(s), flags=re.I)
                )
            )
            area = area.fillna(pd.to_numeric(area_from_desc, errors="coerce"))

        df["area_sqm"] = pd.to_numeric(area, errors="coerce")
    else:
        df["area_sqm"] = pd.to_numeric(df["area_sqm"], errors="coerce")

    # ---------- PROPERTY TYPE ----------
    if "property_type" not in df.columns:
        candidates = [c for c in df.columns if "type" in c.lower()]
        df["property_type"] = df[candidates[0]].astype(str) if candidates else "Unknown"

    # ---------- ADDRESS ----------
    if "address" not in df.columns:
        df["address"] = ""

    # ---------- PPSQM ----------
    if "price_per_sqm" not in df.columns:
        df["price_per_sqm"] = np.where(
            (pd.to_numeric(df["price_php"], errors="coerce").notna()) &
            (pd.to_numeric(df["area_sqm"], errors="coerce") > 0),
            df["price_php"] / df["area_sqm"],
            np.nan,
        )

    for col in ["price_php", "area_sqm", "price_per_sqm"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------- safe load & UI ----------

# Load once
df_all = load_data(DB_PATH)

# ---------- STRICT FILTER: only keep complete rows for display ----------
required_cols = ["listing_title", "price_php", "area_sqm", "price_per_sqm", "address"]

# Ensure required columns exist (create empty if missing)
for col in required_cols:
    if col not in df_all.columns:
        df_all[col] = np.nan

# Normalize None -> NaN uniformly
df_all = df_all.where(pd.notnull(df_all), np.nan)

# Force numeric for metrics/filters
df_all["price_php"] = pd.to_numeric(df_all["price_php"], errors="coerce")
df_all["area_sqm"]  = pd.to_numeric(df_all["area_sqm"],  errors="coerce")
df_all["price_per_sqm"] = pd.to_numeric(df_all["price_per_sqm"], errors="coerce")

# Build a cleaned view for the table (don’t mutate df_all)
df_view = df_all.copy()

# Drop rows with any missing required field
df_view = df_view.dropna(subset=required_cols)

# Also drop rows with non-positive area or price
df_view = df_view[(df_view["area_sqm"] > 0) & (df_view["price_php"] > 0)]

# Recompute price_per_sqm (defensive)
df_view["price_per_sqm"] = df_view["price_php"] / df_view["area_sqm"]

# Replace ±inf safely without FutureWarning
# Replace +/-inf only in numeric columns, then drop bad PPSQM rows
num_cols = df_view.select_dtypes(include=[np.number]).columns
df_view[num_cols] = df_view[num_cols].replace([np.inf, -np.inf], np.nan)
df_view = df_view.dropna(subset=["price_per_sqm"])


# -----------------------------
# Now use df_view for filters + table
# -----------------------------
# Sidebar filters on df_view
with st.sidebar:
    st.header("Filters")
    types = sorted([t for t in df_view["property_type"].dropna().unique().tolist() if t != ""])
    selected_types = st.multiselect("Property Type", options=types, default=types)

    price_series = df_view["price_php"].dropna()
    if not price_series.empty:
        pmin, pmax = int(price_series.min()), int(price_series.max())
        slider_min, slider_max = st.slider(
            "Price (PHP per month)",
            min_value=pmin, max_value=pmax,
            value=(pmin, pmax),
            step=max(1, (pmax - pmin) // 100),
        )
    else:
        slider_min, slider_max = 0, 0
        st.info("No price data available to build a slider.")

    address_query = st.text_input("Search Address / Neighborhood").strip()

# Apply filters
df_filtered = df_view.copy()
if selected_types:
    df_filtered = df_filtered[df_filtered["property_type"].isin(selected_types)]
if slider_max > slider_min:
    df_filtered = df_filtered[df_filtered["price_php"].between(slider_min, slider_max, inclusive="both")]
if address_query:
    df_filtered = df_filtered[df_filtered["address"].fillna("").str.contains(address_query, case=False, na=False)]

# Table
display_cols = ["listing_title", "price_php", "area_sqm", "price_per_sqm", "address"]
for c in display_cols:
    if c not in df_filtered.columns:
        df_filtered[c] = np.nan

st.dataframe(
    df_filtered[display_cols].sort_values(by=["price_php"], ascending=True, na_position="last"),
    use_container_width=True,
)

