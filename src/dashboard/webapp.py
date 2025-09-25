
import os
from datetime import timezone
from typing import Optional, List

import numpy as np
import pandas as pd
import streamlit as st
from supabase import create_client

# -------------------- App config --------------------
st.set_page_config(page_title="PH Office Listings", page_icon="🏢", layout="wide")

SUPABASE_URL = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_ANON_KEY")
DEFAULT_SOURCE = st.secrets.get("SOURCE") or os.getenv("SOURCE") or "lamudi_cebu"
TIMEZONE = "Asia/Manila"

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Supabase credentials are missing. Set SUPABASE_URL and SUPABASE_ANON_KEY in Streamlit secrets or env.")
    st.stop()

# -------------------- Helpers -----------------------
@st.cache_data(ttl=600)
def get_client():
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

def _to_ts(x: Optional[str]) -> Optional[pd.Timestamp]:
    if not x:
        return None
    try:
        return pd.to_datetime(x, utc=True, errors="coerce")
    except Exception:
        return None

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

@st.cache_data(ttl=600, show_spinner="Loading listings from Supabase…")
def load_data(source: str, limit: int = 5000) -> pd.DataFrame:
    sb = get_client()
    cols = [
        "url","listing_title","address","property_type",
        "price_php","area_sqm","price_per_sqm",
        "published_at","published_at_text","scraped_at","source",
    ]
    resp = (sb.table("listings")
              .select(",".join(cols))
              .eq("source", source)
              .order("scraped_at", desc=True)
              .limit(limit)
              .execute())
    df = pd.DataFrame(resp.data or [])
    if df.empty:
        return df

    # Normalize/derived
    df["city"] = df["address"].apply(extract_city)
    # Compute price_per_sqm if missing and both parts exist
    need_pps = df["price_per_sqm"].isna() if "price_per_sqm" in df else True
    if "price_php" in df and "area_sqm" in df:
        can_compute = df["price_php"].notna() & df["area_sqm"].notna() & (df["area_sqm"] > 0)
        df.loc[need_pps & can_compute, "price_per_sqm"] = df["price_php"] / df["area_sqm"]

    # Filter date = published_at if present else scraped_at
    df["filter_date"] = [coalesce_datetime(pa, sa) for pa, sa in zip(df.get("published_at"), df.get("scraped_at"))]
    if df["filter_date"].notna().any():
        df["filter_date_local"] = df["filter_date"].dt.tz_convert(TIMEZONE).dt.strftime("%Y-%m-%d %H:%M")
    else:
        df["filter_date_local"] = None

    return df

# -------------------- UI ---------------------------
st.title("🏢 Cebu Office Listings — Analytics")

with st.sidebar:
    st.header("Data Source & Cache")
    source = st.text_input("Source tag (equals listings.source)", value=DEFAULT_SOURCE)
    if st.button("🔄 Refresh data"):
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
    cities = sorted([c for c in df["city"].dropna().unique().tolist()])
    city_sel: List[str] = st.multiselect("City", options=cities, default=cities or [])

    # Property type filter
    ptypes = sorted([p for p in df["property_type"].dropna().unique().tolist()])
    type_sel: List[str] = st.multiselect("Property type", options=ptypes, default=ptypes or [])

    # Price/sqm slider with robust bounds
    pps = df["price_per_sqm"].dropna().astype(float)
    if len(pps):
        lo = float(np.nanpercentile(pps, 1))
        hi = float(np.nanpercentile(pps, 99))
        min_pps, max_pps = st.slider("Price per sqm (PHP)", min_value=0.0, max_value=max(hi, lo),
                                     value=(lo, hi), step=50.0)
    else:
        min_pps, max_pps = 0.0, float("inf")
        st.caption("No price/sqm values yet; slider disabled.")

    # Date range
    if df["filter_date"].notna().any():
        dmin = pd.to_datetime(df["filter_date"].min()).date()
        dmax = pd.to_datetime(df["filter_date"].max()).date()
        d_from, d_to = st.date_input("Date range (published/scraped)", value=(dmin, dmax),
                                     min_value=dmin, max_value=dmax)
    else:
        d_from = d_to = None
        st.caption("No date metadata present to filter by.")

    # Free text search
    q = st.text_input("Search text (title/address)", "")

# -------------------- Apply filters ----------------
flt = df.copy()

if city_sel:
    flt = flt[flt["city"].isin(city_sel)]
if type_sel:
    flt = flt[flt["property_type"].isin(type_sel)]

if "price_per_sqm" in flt and len(flt):
    flt = flt[
        flt["price_per_sqm"].astype(float).fillna(-1).between(
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
        flt["listing_title"].fillna("").str.lower().str.contains(qq)
        | flt["address"].fillna("").str.lower().str.contains(qq)
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
