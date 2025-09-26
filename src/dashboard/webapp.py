import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # repo root (…/RealEstate_Scraper)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ------------------------------------------------------------------------

from src.db.supabase_client import get_client

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Listings Dashboard", layout="wide")

# ---- Controls
st.title("Real Estate Scraper — Dashboard")
col_f, col_t, col_src = st.columns([1,1,1.2])
with col_f:
    days_back = st.number_input("Days back", min_value=1, max_value=90, value=14, step=1)
with col_t:
    min_area = st.number_input("Min area (sqm)", min_value=0, value=0, step=5)
with col_src:
    portal = st.text_input("Source (portal)", "lamudi_cebu")

# ---- Data
sb = get_client()
since = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()

# Pull a bit more than we need; we’ll filter in pandas too
cols = "url,listing_title,address,property_type,price_php,area_sqm,price_per_sqm,published_at,scraped_at,source"
resp = (
    sb.table("listings")
      .select(cols)
      .gte("scraped_at", since)
      .eq("source", portal)
      .order("scraped_at", desc=True)
      .limit(5000)
      .execute()
)

df = pd.DataFrame(resp.data or [])
if df.empty:
    st.info("No rows returned for the current filters.")
    st.stop()

# Clean/derive
df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True)
df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
df = df[df["area_sqm"].fillna(0) >= min_area]

# ---- KPIs
k1, k2, k3, k4 = st.columns(4)
with k1: st.metric("Rows", len(df))
with k2: st.metric("Median PHP", f"{df['price_php'].median(skipna=True):,.0f}" if df["price_php"].notna().any() else "—")
with k3: st.metric("Median sqm", f"{df['area_sqm'].median(skipna=True):,.0f}" if df["area_sqm"].notna().any() else "—")
with k4: st.metric("Median PHP/sqm", f"{df['price_per_sqm'].median(skipna=True):,.0f}" if df["price_per_sqm"].notna().any() else "—")

st.divider()

# ---- Charts (both in one file, side-by-side)
left, right = st.columns(2)

# Chart A: New listings per day (by scraped_at)
with left:
    st.subheader("New listings per day")
    daily = (
        df.assign(day=df["scraped_at"].dt.tz_convert(None).dt.date)
          .groupby("day", dropna=False)
          .size()
          .reset_index(name="count")
          .sort_values("day")
    )
    st.bar_chart(data=daily, x="day", y="count", use_container_width=True)

# Chart B: Price vs Area scatter (with optional color by property_type)
with right:
    st.subheader("Price vs. Area (PHP vs sqm)")
    scatter = df[["price_php", "area_sqm", "property_type", "listing_title"]].dropna(subset=["price_php","area_sqm"])
    if scatter.empty:
        st.info("No rows with both price and area.")
    else:
        # Streamlit's native chart API is simple; if you prefer matplotlib/plotly, swap here.
        st.scatter_chart(
            scatter,
            x="area_sqm",
            y="price_php",
            color="property_type" if scatter["property_type"].notna().any() else None,
            size=None,
            use_container_width=True
        )

st.divider()
st.caption(f"Source: {portal} • Showing last {days_back} day(s) • Min area ≥ {min_area} sqm")


