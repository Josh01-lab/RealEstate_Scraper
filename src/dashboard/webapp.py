from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[2]  # repo root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ---------------------------------------------------------------------------

from zoneinfo import ZoneInfo
import os
from datetime import timezone
from typing import Optional, List

import numpy as np
import pandas as pd
import streamlit as st
from supabase import create_client
from src.db.supabase_client import get_client as _raw_get_client


# -------------------- App config --------------------
st.set_page_config(page_title="PH Office Listings", page_icon="", layout="wide")

SUPABASE_URL = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_ANON_KEY")
DEFAULT_SOURCE = st.secrets.get("SOURCE") or os.getenv("SOURCE") or "lamudi_cebu"
TIMEZONE = "Asia/Manila"

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Supabase credentials are missing. Set SUPABASE_URL and SUPABASE_ANON_KEY in Streamlit secrets or env.")
    st.stop()

REQUIRED_COLS = [
    "url",
    "listing_title",
    "address",
    "property_type",
    "price_php",
    "area_sqm",
    "price_per_sqm",
    "published_at",
    "published_at_text",
    "description",
    "scraped_at",
    "source",
]
   

# -------------------- Helpers -----------------------
@st.cache_resource
def get_client():
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

@st.cache_data(ttl=300)  # 5 minutes; adjust as you like
def fetch_listings(source: str) -> pd.DataFrame:
    sb = get_client()
    rows = (sb.table("listings")
              .select("*")
              .eq("source", source)
              .order("scraped_at", desc=True)
              .limit(5000)
              .execute()
              .data)
    return pd.DataFrame(rows)

def _to_ts(x: Optional[str]) -> Optional[pd.Timestamp]:
    if not x:
        return None
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return None
def _to_dt_safe(x):
    # Normalize ISO strings / None / NaN to pandas timestamps (UTC)
    df = client.table("listings").select("*").execute().data
    df = pd.DataFrame(df)

    if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and not x.strip()):
        return pd.NaT
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return pd.NaT

# Ensure columns exist; if missing, create empty with NaT
if "published_at" not in df.columns:
    df["published_at"] = None
if "scraped_at" not in df.columns:
    df["scraped_at"] = pd.NaT

# Parse to datetimes
df["published_at_dt"] = df["published_at"].apply(_to_dt_safe)
df["scraped_at_dt"]   = df["scraped_at"].apply(_to_dt_safe)

# Coalesce: prefer published_at, else scraped_at
df["filter_date"] = df["published_at_dt"].fillna(df["scraped_at_dt"])

# Optional: if you use filter_date for sorting/filters and want strings
# df["filter_date_str"] = df["filter_date"].dt.strftime("%Y-%m-%d %H:%M:%S%z").fillna("")

def coalesce_datetime(a: Optional[str], b: Optional[str]) -> Optional[pd.Timestamp]:
    # prefer published_at; else scraped_at
    return _to_ts(a) or _to_ts(b)

def extract_city(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    parts = [p.strip() for p in str(address).split(",") if p and p.strip()]
    return parts[-1] if parts else None

def _fmt_php(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    try:
        return f"₱ {int(round(float(x))):,}"
    except Exception:
        return "—"

# --- fetch data ---
@st.cache_data(ttl=300)
def load_rows() -> list[dict]:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        st.error("Supabase credentials missing. Set SUPABASE_URL and a key in env.")
        return []
    sb: Client = create_client(url, key)
    # Pull what you need; * includes all columns so published_at is included if it exists
    resp = (sb.table("listings")
              .select("*")
              .order("scraped_at", desc=True)
              .limit(500)
              .execute())
    return resp.data or []

df = load_rows()

# --- guard rails so df always exists and has expected columns ---
if df.empty:
    st.info("No data available yet.")
    st.stop()

# Ensure columns exist even if missing in some rows/schema
for col, default in [
    ("published_at_text", None),
    ("published_at", None),
    ("description", None),
    ("price_php", None),
    ("area_sqm", None),
    ("price_per_sqm", None),
]:
    if col not in df.columns:
        df[col] = default

# Types / derived fields
if "published_at" in df.columns:
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
if "scraped_at" in df.columns:
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True)
if {"price_php", "area_sqm"}.issubset(df.columns):
    with pd.option_context("mode.use_inf_as_na", True):
        df["price_per_sqm"] = (
            pd.to_numeric(df["price_php"], errors="coerce") /
            pd.to_numeric(df["area_sqm"], errors="coerce")
        )

# from here on, df is safe to use
# example:
st.dataframe(df[["url","listing_title","address","published_at","scraped_at","price_php","area_sqm","price_per_sqm"]])


@st.cache_data(show_spinner=False, ttl=300)
def load_listings_df(source_filter: str = None, limit: int = 1000) -> pd.DataFrame:
    """
    Best-effort load from Supabase. Never raises; always returns a DataFrame.
    """
    try:
        sb = get_client()
        q = sb.table("listings").select("*")
        if source_filter:
            q = q.eq("source", source_filter)
        # Order newest first
        res = q.order("scraped_at", desc=True).limit(limit).execute()
        data = res.data or []
        df = pd.DataFrame(data)
    except Exception as e:
        # Log to Streamlit and fall back to empty frame
        st.warning(f"Could not load data from Supabase: {e}")
        df = pd.DataFrame()

    # Ensure required columns exist (avoid KeyError/NameError later)
    for col in REQUIRED_COLS:
        if col not in df.columns:
            df[col] = None

    # Coerce types safely
    if not df.empty:
        # published_at/scraped_at to datetimes
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
        df["scraped_at"]   = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True)
        # numerics
        for col in ["price_php", "area_sqm", "price_per_sqm"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

# ----- main app code -----
st.set_page_config(page_title="Listings Dashboard", layout="wide")

# Example: filter for your portal
portal = st.sidebar.selectbox("Source", ["lamudi_cebu", "all"], index=0)
source_filter = None if portal == "all" else portal

df = load_listings_df(source_filter=source_filter, limit=2000)

# From here on, df is ALWAYS defined
if df.empty:
    st.info("No data yet. Try running the scraper or widening your filters.")
else:
    # Your existing UI/metrics/tables follow...
    pass


# -------------------- UI ---------------------------
st.title(" Cebu Office Listings — Analytics")

with st.sidebar:
    st.header("Data Source & Cache")
    source = st.text_input("Source tag (equals listings.source)", value=DEFAULT_SOURCE)
    if st.button(" Refresh data"):
        get_client.clear()
        load_data.clear()
        st.experimental_rerun()

df = load_data(source=source)
if df.empty:
    st.info("No data yet for this source. Once the scraper publishes rows, they’ll show up here.")
    st.stop()

with st.sidebar:
    st.header("Filters")

    # City filter
    cities = sorted([c for c in df.get("city", pd.Series(dtype=str)).dropna().unique().tolist()])
    city_sel: List[str] = st.multiselect("City", options=cities, default=cities)

    # Property type filter
    ptypes = sorted([p for p in df.get("property_type", pd.Series(dtype=str)).dropna().unique().tolist()])
    type_sel: List[str] = st.multiselect("Property type", options=ptypes, default=ptypes)


    # Price/sqm slider with robust bounds
    pps = pd.to_numeric(df.get("price_per_sqm"), errors="coerce")
    pps = pps[pps.notna() & np.isfinite(pps)]
   
    if len(pps) >= 3:
        lo = float(np.nanpercentile(pps, 1))
        hi = float(np.nanpercentile(pps, 99))
        if not np.isfinite(lo) or not np.isfinite(hi) or lo >= hi:
            lo = float(pps.min())
            hi = float(pps.max())
    elif len(pps) == 2:
        lo, hi = float(pps.min()), float(pps.max())
    elif len(pps) == 1:
        lo = hi = float(pps.iloc[0])
    else:
        lo = hi = None
   
    if lo is not None and hi is not None and np.isfinite(lo) and np.isfinite(hi):
        # widen a tiny bit if both equal to keep Streamlit happy
        if lo == hi:
            hi = lo + 1.0
        min_pps, max_pps = st.slider(
            "Price per sqm (PHP)",
            min_value=float(max(0.0, min(lo, hi, default=0.0))),
            max_value=float(max(lo, hi)),
            value=(float(lo), float(hi)),
            step=50.0,
        )
    else:
        min_pps, max_pps = 0.0, float("inf")
        st.caption("No price/sqm values yet; slider disabled.")

    # Date range
    if df["filter_date"].notna().any():
        dmin = pd.to_datetime(df["filter_date"].min()).date()
        dmax = pd.to_datetime(df["filter_date"].max()).date()
        picked = st.date_input(
            "Date range (published/scraped)",
            value=(dmin, dmax),
            min_value=dmin, max_value=dmax
        )
        # Streamlit can return a single date if ranges collapse; normalize
        if isinstance(picked, tuple) and len(picked) == 2:
            d_from, d_to = picked
        else:
            d_from = d_to = picked
    else:
        d_from = d_to = None
        st.caption("No date metadata present to filter by.")
   
    # Apply date filter (inclusive, tz-aware)
    if d_from and d_to and "filter_date" in flt.columns:
        start = pd.Timestamp(d_from, tz="UTC")
        end = pd.Timestamp(d_to, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        mask_date = flt["filter_date"].notna() & flt["filter_date"].between(start, end)
        flt = flt[mask_date]

# -------------------- Apply filters ----------------
flt = df.copy()

if city_sel:
    flt = flt[flt.get("city").isin(city_sel)]
if type_sel:
    flt = flt[flt.get("property_type").isin(type_sel)]

if "price_per_sqm" in flt.columns and len(flt):
    flt = flt[
        pd.to_numeric(flt["price_per_sqm"], errors="coerce")
        .fillna(-1)
        .between(
            min_pps if np.isfinite(min_pps) else -1,
            max_pps if np.isfinite(max_pps) else float("inf")
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
        flt.get("listing_title", pd.Series("", index=flt.index)).fillna("").str.lower().str.contains(qq)
        | flt.get("address", pd.Series("", index=flt.index)).fillna("").str.lower().str.contains(qq)
    ]

# -------------------- KPIs -------------------------
left, mid, right, right2 = st.columns(4)
with left:
    st.metric("Listings (filtered)", len(flt))
with mid:
    st.metric("Median PHP/sqm", _fmt_php(np.nanmedian(flt["price_per_sqm"])) if flt["price_per_sqm"].notna().any() else "—")
with right:
    st.metric("Avg PHP/sqm", _fmt_php(np.nanmean(flt["price_per_sqm"])) if flt["price_per_sqm"].notna().any() else "—")
with right2:
    st.metric("Cities", int(flt["city"].nunique()))

st.divider()

# -------------------- Table ------------------------
view_cols = [
    "listing_title", "city", "address", "property_type",
    "price_php", "area_sqm", "price_per_sqm",
    "filter_date_local", "url",
]
present = [c for c in view_cols if c in flt.columns]

st.subheader("Results")
st.caption("Click column headers to sort. Use filters in the sidebar to narrow results.")
show = flt[present].rename(columns={
    "filter_date_local": "date",
    "price_php": "price (PHP)",
    "area_sqm": "area (sqm)",
    "price_per_sqm": "PHP/sqm",
})
st.dataframe(show, use_container_width=True, hide_index=True)

# -------------------- Viz --------------------------
if flt["price_per_sqm"].notna().sum() >= 5:
    st.subheader("Distribution of Price per sqm")
    st.bar_chart(flt["price_per_sqm"].dropna())

# -------------------- Download ---------------------
csv = show.to_csv(index=False).encode("utf-8")
st.download_button("Download CSV", csv, file_name="listings_filtered.csv", mime="text/csv")

st.caption("Data source: Supabase • Date = published_at if present, else scraped_at • Currency = PHP")






