from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datetime import timezone
from zoneinfo import ZoneInfo
from typing import Optional, List
import os

import numpy as np
import pandas as pd
import streamlit as st
from supabase import create_client

# ---------- App config ----------
st.set_page_config(page_title="PH Office Listings", page_icon="", layout="wide")

SUPABASE_URL = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_ANON_KEY")
DEFAULT_SOURCE = st.secrets.get("SOURCE") or os.getenv("SOURCE") or "lamudi_cebu"
TZ = ZoneInfo("Asia/Manila")

REQUIRED_COLS = [
    "url", "listing_title", "address", "property_type",
    "price_php", "area_sqm", "price_per_sqm",
    "published_at", "published_at_text", "description",
    "scraped_at", "source",
]
df = pd.DataFrame(rows)

try:
    from dotenv import load_dotenv
    ROOT = Path(_file_).resolve().parents[1]
    load_dotenv(ROOT / ".env")
except Exception:
    pass

from src.db.supabase_client import get_client

# Be tolerant with columns (table may not have every field yet)
SAFE_COLUMNS = (
    "url,listing_title,address,property_type,price_php,area_sqm,"
    "price_per_sqm,published_at,scraped_at,source,description"
)

sb = get_client()

def fetch_rows(limit=50):
    # Use a safe field list; if some columns don’t exist yet, fall back to "*"
    try:
        return (sb.table("listings")
                  .select(SAFE_COLUMNS)
                  .order("scraped_at", desc=True)
                  .limit(limit)
                  .execute().data)
    except Exception:
        return (sb.table("listings")
                  .select("*")
                  .order("scraped_at", desc=True)
                  .limit(limit)
                  .execute().data)

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Supabase credentials are missing. Set SUPABASE_URL and SUPABASE_ANON_KEY in Streamlit secrets or env.")
    st.stop()

# ---------- Supabase client ----------
@st.cache_resource
def get_client():
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# ---------- Helpers ----------
def _to_utc_ts(x: Optional[str]) -> pd.Timestamp | pd.NaT:
    if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and not x.strip()):
        return pd.NaT
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return pd.NaT

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

# ---------- Data access ----------
@st.cache_data(ttl=300)
def load_listings_df(source_filter: Optional[str] = None, limit: int = 2000) -> pd.DataFrame:
    """
    Best-effort load from Supabase. Always returns a DataFrame with REQUIRED_COLS present.
    """
    try:
        sb = get_client()
        q = sb.table("listings").select("*")
        if source_filter:
            q = q.eq("source", source_filter)
        res = q.order("scraped_at", desc=True).limit(limit).execute()
        rows = res.data or []
        df = pd.DataFrame(rows)
    except Exception as e:
        st.warning(f"Could not load data from Supabase: {e}")
        df = pd.DataFrame()

    # Guarantee expected columns exist
    for col in REQUIRED_COLS:
        if col not in df.columns:
            df[col] = None

    if df.empty:
        return df

    # Coerce datetimes
    df["published_at"] = df["published_at"].apply(_to_utc_ts)
    df["scraped_at"] = df["scraped_at"].apply(_to_utc_ts)

    # Compute price_per_sqm if missing
    if "price_per_sqm" in df.columns:
        with pd.option_context("mode.use_inf_as_na", True):
            df["price_per_sqm"] = (
                pd.to_numeric(df["price_php"], errors="coerce") /
                pd.to_numeric(df["area_sqm"], errors="coerce")
            )

    # Derive city
    if "city" not in df.columns:
        df["city"] = df["address"].apply(extract_city)

    # Coalesce filter date: prefer published_at else scraped_at
    df["filter_date"] = df["published_at"].fillna(df["scraped_at"])
    # local display date
    df["filter_date_local"] = pd.to_datetime(df["filter_date"]).dt.tz_convert(TZ)

    return df

@st.cache_data(ttl=300)
def list_sources() -> List[str]:
    try:
        sb = get_client()
        rows = sb.table("listings").select("source").execute().data or []
        srcs = sorted({r.get("source") for r in rows if r.get("source")})
        return srcs or [DEFAULT_SOURCE]
    except Exception:
        return [DEFAULT_SOURCE]

# ---------- Sidebar ----------
st.title("Cebu Office Listings — Analytics")

sources = list_sources()
portal = st.sidebar.selectbox("Source", ["all", *sources], index=(0 if DEFAULT_SOURCE not in sources else sources.index(DEFAULT_SOURCE)+1))
source_filter = None if portal == "all" else portal

df = load_listings_df(source_filter=source_filter, limit=5000)

if df.empty:
    st.info("No data yet. Once the scraper publishes rows, they’ll show up here.")
    st.stop()

with st.sidebar:
    st.header("Filters")

    # City filter
    cities = sorted([c for c in df.get("city", pd.Series(dtype=str)).dropna().unique().tolist()])
    city_sel: List[str] = st.multiselect("City", options=cities, default=cities)

    # Property type filter
    ptypes = sorted([p for p in df.get("property_type", pd.Series(dtype=str)).dropna().unique().tolist()])
    type_sel: List[str] = st.multiselect("Property type", options=ptypes, default=ptypes)

    # Price/sqm slider
    pps = pd.to_numeric(df.get("price_per_sqm"), errors="coerce")
    pps = pps[pps.notna() & np.isfinite(pps)]
    if len(pps) >= 1:
        lo = float(np.nanpercentile(pps, 1)) if len(pps) >= 3 else float(pps.min())
        hi = float(np.nanpercentile(pps, 99)) if len(pps) >= 3 else float(pps.max())
        if lo == hi:
            hi = lo + 1.0
        min_pps, max_pps = st.slider("Price per sqm (PHP)", min_value=float(max(0.0, lo)), max_value=float(hi), value=(float(lo), float(hi)), step=50.0)
    else:
        min_pps, max_pps = 0.0, float("inf")
        st.caption("No price/sqm values yet; slider disabled.")

    # Date range (published/scraped coalesced)
    if df["filter_date"].notna().any():
        dmin = df["filter_date_local"].min().date()
        dmax = df["filter_date_local"].max().date()
        dr = st.date_input("Date range (published/scraped)", value=(dmin, dmax), min_value=dmin, max_value=dmax)
        if isinstance(dr, tuple) and len(dr) == 2:
            d_from, d_to = dr
        else:
            d_from = d_to = dr
    else:
        d_from = d_to = None
        st.caption("No date metadata present to filter by.")

    # Keyword
    q = st.text_input("Search (title/address contains)")

# ---------- Apply filters ----------
flt = df.copy()

if city_sel:
    flt = flt[flt.get("city").isin(city_sel)]

if type_sel:
    flt = flt[flt.get("property_type").isin(type_sel)]

if "price_per_sqm" in flt.columns and len(flt):
    flt = flt[
        pd.to_numeric(flt["price_per_sqm"], errors="coerce")
        .fillna(-1)
        .between(min_pps, max_pps)
    ]

if d_from and d_to and "filter_date_local" in flt.columns:
    start = pd.Timestamp(d_from, tz=TZ)
    end = pd.Timestamp(d_to, tz=TZ) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    mask_date = flt["filter_date_local"].notna() & flt["filter_date_local"].between(start, end)
    flt = flt[mask_date]

if q.strip():
    qq = q.lower()
    flt = flt[
        flt.get("listing_title", pd.Series("", index=flt.index)).fillna("").str.lower().str.contains(qq)
        | flt.get("address", pd.Series("", index=flt.index)).fillna("").str.lower().str.contains(qq)
    ]

# ---------- KPIs ----------
left, mid, right, right2 = st.columns(4)
with left:
    st.metric("Listings (filtered)", len(flt))
with mid:
    if flt["price_per_sqm"].notna().any():
        st.metric("Median PHP/sqm", _fmt_php(float(np.nanmedian(flt["price_per_sqm"]))))
    else:
        st.metric("Median PHP/sqm", "—")
with right:
    if flt["price_per_sqm"].notna().any():
        st.metric("Avg PHP/sqm", _fmt_php(float(np.nanmean(flt["price_per_sqm"]))))
    else:
        st.metric("Avg PHP/sqm", "—")
with right2:
    st.metric("Cities", int(flt["city"].dropna().nunique()))

st.divider()

# ---------- Table ----------
view_cols = [
    "listing_title", "city", "address", "property_type",
    "price_php", "area_sqm", "price_per_sqm",
    "filter_date_local", "url",
]
present = [c for c in view_cols if c in flt.columns]
show = flt[present].rename(columns={
    "filter_date_local": "date (Asia/Manila)",
    "price_php": "price (PHP)",
    "area_sqm": "area (sqm)",
    "price_per_sqm": "PHP/sqm",
})
st.subheader("Results")
st.caption("Click column headers to sort. Use filters in the sidebar to narrow results.")
st.dataframe(show, use_container_width=True, hide_index=True)

# ---------- Viz ----------
if flt["price_per_sqm"].notna().sum() >= 5:
    st.subheader("Distribution of Price per sqm")
    st.bar_chart(flt["price_per_sqm"].dropna())

# ---------- Download ----------
csv = show.to_csv(index=False).encode("utf-8")
st.download_button("Download CSV", csv, file_name="listings_filtered.csv", mime="text/csv")

st.caption("Data source: Supabase • Date = published_at if present, else scraped_at • Currency = PHP")



