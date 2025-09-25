import os
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st
from supabase import create_client

# ---------- Config ----------
st.set_page_config(page_title="PH Office Listings", page_icon="🏢", layout="wide")

SUPABASE_URL = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_ANON_KEY")
SOURCE = st.secrets.get("SOURCE", "lamudi_cebu") # change or override in secrets

# ---------- Helpers ----------
@st.cache_data(ttl=600)
def get_client():
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

def coalesce_datetime(a: Optional[str], b: Optional[str]) -> Optional[pd.Timestamp]:
    # prefer published_at; else scraped_at
    for v in (a, b):
        if not v:
            continue
        try:
            return pd.to_datetime(v, utc=True, errors="coerce")
        except Exception:
            pass
    return None

def extract_city(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    # Simple, robust heuristic: take the last token after the final comma
    # "Cebu IT Park, Cebu" -> "Cebu"
    parts = [p.strip() for p in address.split(",") if p.strip()]
    return parts[-1] if parts else None

@st.cache_data(ttl=600, show_spinner="Loading listings from Supabase…")
def load_data(limit: int = 5000) -> pd.DataFrame:
    sb = get_client()
    # Pull minimal-but-useful columns; add more if you need them in the grid
    cols = [
        "url", "listing_title", "address", "property_type",
        "price_php", "area_sqm", "price_per_sqm",
        "published_at", "published_at_text", "scraped_at", "source",
    ]
    resp = sb.table("listings").select(",".join(cols)).eq("source", SOURCE).limit(limit).order("scraped_at", desc=True).execute()
    df = pd.DataFrame(resp.data or [])
    if df.empty:
        return df

    # Normalize/compute derived fields
    df["city"] = df["address"].apply(extract_city)
    # prefer existing price_per_sqm; compute if missing and both price + area exist
    need_pps = df["price_per_sqm"].isna()
    can_compute = df["price_php"].notna() & df["area_sqm"].notna() & (df["area_sqm"] > 0)
    df.loc[need_pps & can_compute, "price_per_sqm"] = df["price_php"] / df["area_sqm"]

    # Date for filter: prefer published_at else scraped_at
    df["filter_date"] = [
        coalesce_datetime(pa, sa) for pa, sa in zip(df.get("published_at"), df.get("scraped_at"))
    ]
    # For display
    if "filter_date" in df:
        df["filter_date_local"] = df["filter_date"].dt.tz_convert("Asia/Manila").dt.strftime("%Y-%m-%d %H:%M")
    return df

def kpi(label: str, value, help_text: Optional[str] = None):
    st.metric(label, value if value is not None else "—", help=help_text)

# ---------- UI: Sidebar Filters ----------
st.title("🏢 Cebu Office Listings — Analytics")

df = load_data()
if df.empty:
    st.info("No data yet. Scrape needs to publish rows to Supabase.")
    st.stop()

with st.sidebar:
    st.header("Filters")

    # Cities from data, sorted; allow multi-select
    cities = sorted([c for c in df["city"].dropna().unique()])
    city_sel = st.multiselect("City", options=cities, default=cities)

    # Price/sqm slider (robust to NaNs/outliers)
    pps = df["price_per_sqm"].dropna()
    if pps.empty:
        min_pps, max_pps = 0.0, 0.0
    else:
        # Clip to sane range using percentiles to avoid extreme outliers breaking UI
        lo = float(np.nanpercentile(pps, 1))
        hi = float(np.nanpercentile(pps, 99))
        min_pps, max_pps = st.slider("Price per sqm (PHP)", min_value=0.0, max_value=max(hi, lo), value=(lo, hi), step=50.0)

    # Date range (published_at preferred; else scraped_at)
    if df["filter_date"].notna().any():
        dmin = pd.to_datetime(df["filter_date"].min()).date()
        dmax = pd.to_datetime(df["filter_date"].max()).date()
        d_from, d_to = st.date_input("Date range", value=(dmin, dmax), min_value=dmin, max_value=dmax)
    else:
        d_from = d_to = None
        st.caption("No date metadata present to filter by.")

    # Free text search by title/address (optional)
    q = st.text_input("Search text (title/address)", "")

# ---------- Apply Filters ----------
flt = df.copy()

if city_sel:
    flt = flt[flt["city"].isin(city_sel)]

if "price_per_sqm" in flt and len(flt):
    flt = flt[
        flt["price_per_sqm"].fillna(-1).between(
            min_pps if 'min_pps' in locals() else -1,
            max_pps if 'max_pps' in locals() else float("inf")
        )
    ]

if d_from and d_to and "filter_date" in flt:
    mask_date = flt["filter_date"].notna() & flt["filter_date"].between(
        pd.Timestamp(d_from, tz=timezone.utc),
        pd.Timestamp(d_to, tz=timezone.utc) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    )
    flt = flt[mask_date]

if q.strip():
    qq = q.lower()
    flt = flt[
        flt["listing_title"].fillna("").str.lower().str.contains(qq)
        | flt["address"].fillna("").str.lower().str.contains(qq)
    ]

# ---------- KPIs ----------
left, mid, right, right2 = st.columns(4)
with left:
    kpi("Listings (filtered)", len(flt))
with mid:
    kpi("Median price/sqm", f"₱ {int(np.nanmedian(flt['price_per_sqm'])):,}" if flt["price_per_sqm"].notna().any() else None)
with right:
    kpi("Avg price/sqm", f"₱ {int(np.nanmean(flt['price_per_sqm'])):,}" if flt["price_per_sqm"].notna().any() else None)
with right2:
    kpi("Cities", flt["city"].nunique())

st.divider()

# ---------- Table ----------
view_cols = [
    "listing_title", "city", "address", "property_type",
    "price_php", "area_sqm", "price_per_sqm",
    "filter_date_local", "url",
]
present = [c for c in view_cols if c in flt.columns]
st.subheader("Results")
st.caption("Click column headers to sort. Use filters in the sidebar to narrow results.")
st.dataframe(
    flt[present].rename(columns={
        "filter_date_local": "date",
        "price_php": "price (PHP)",
        "area_sqm": "area (sqm)",
        "price_per_sqm": "PHP/sqm",
    }),
    use_container_width=True,
    hide_index=True,
)

# ---------- Quick viz (optional) ----------
if flt["price_per_sqm"].notna().sum() >= 5:
    st.subheader("Distribution of Price per sqm")
    st.bar_chart(flt["price_per_sqm"].dropna())

# ---------- Download ----------
csv = flt[present].to_csv(index=False).encode("utf-8")
st.download_button("Download CSV", csv, file_name="listings_filtered.csv", mime="text/csv")

st.caption("Data source: Supabase • Date = published_at if present, else scraped_at • Currency = PHP")
