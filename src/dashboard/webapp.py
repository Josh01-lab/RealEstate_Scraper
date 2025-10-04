import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from dateutil import parser as dtparse
from typing import Tuple, Optional

# Import your supabase client factory (adjust import path if needed)
from src.db.supabase_client import get_client

st.set_page_config(layout="wide", page_title="Cebu Office Listings — Analytics")

# --- helpers ---------------------------------------------------------
@st.cache_data(ttl=60)
def fetch_listings(source_tag: str) -> pd.DataFrame:
    """Fetch listings from Supabase for a source tag and return as DataFrame."""
    sb = get_client()
    # select fields used by the UI; add more if you need
    select_cols = [
        "url",
        "listing_title",
        "address",
        "city",
        "property_type",
        "price_php",
        "area_sqm",
        "price_per_sqm",
        "published_at",
        "scraped_at",
    ]
    resp = sb.table("listings").select(",".join(select_cols)).eq("source", source_tag).execute()
    data = resp.data or []
    df = pd.DataFrame(data)
    # normalize date columns
    for col in ("published_at", "scraped_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    # rename columns for UI clarity
    if "listing_title" in df.columns:
        df = df.rename(columns={"listing_title": "listing_title"})
    # ensure numeric typed
    if "price_php" in df.columns:
        df["price_php"] = pd.to_numeric(df["price_php"], errors="coerce")
    if "area_sqm" in df.columns:
        df["area_sqm"] = pd.to_numeric(df["area_sqm"], errors="coerce")
    if "price_per_sqm" in df.columns:
        df["price_per_sqm"] = pd.to_numeric(df["price_per_sqm"], errors="coerce")
    # derive city if missing from address (best-effort)
    if "city" not in df.columns and "address" in df.columns:
        df["city"] = df["address"].fillna("").str.extract(r"(Cebu(?:\s+City)?|Mandaue|Lapu[- ]?Lapu)", expand=False).fillna("Unknown")
    return df

def filter_df(df: pd.DataFrame,
              property_type: Optional[str],
              city: Optional[str],
              price_min: float,
              price_max: float,
              area_min: float,
              area_max: float,
              published_from: Optional[pd.Timestamp],
              published_to: Optional[pd.Timestamp]) -> pd.DataFrame:
    d = df.copy()
    if property_type:
        d = d[d["property_type"].fillna("").str.contains(property_type, case=False, na=False)]
    if city:
        d = d[d["city"].fillna("").str.contains(city, case=False, na=False)]
    if price_min is not None:
        d = d[d["price_php"].fillna(0) >= price_min]
    if price_max is not None and price_max > 0:
        d = d[d["price_php"].fillna(0) <= price_max]
    if area_min is not None and area_min > 0:
        d = d[d["area_sqm"].fillna(0) >= area_min]
    if area_max is not None and area_max > 0:
        d = d[d["area_sqm"].fillna(0) <= area_max]
    if published_from is not None:
        d = d[d["published_at"] >= published_from]
    if published_to is not None:
        d = d[d["published_at"] <= published_to]
    return d

# --- UI --------------------------------------------------------------
st.title("Cebu Office Listings — Analytics")

# left sidebar filters
with st.sidebar:
    st.header("Data Source & Cache")
    source = st.selectbox("Source tag (equals listings.source)", ["lamudi_cebu"], index=0)
    if st.button("Refresh data"):
        fetch_listings.clear()

    st.markdown("---")
    st.header("Filters")
    city_filter = st.text_input("City", value="Cebu")
    prop_type_filter = st.text_input("Property type", value="Offices")
    st.markdown("Price (PHP)")
    col1, col2 = st.columns(2)
    price_min = col1.number_input("Min price (PHP)", min_value=0.0, value=0.0, step=1000.0)
    price_max = col2.number_input("Max price (PHP, 0 = no limit)", min_value=0.0, value=0.0, step=1000.0)
    st.markdown("Area (sqm)")
    col3, col4 = st.columns(2)
    area_min = col3.number_input("Min area (sqm)", min_value=0.0, value=0.0, step=1.0)
    area_max = col4.number_input("Max area (sqm, 0 = no limit)", min_value=0.0, value=0.0, step=1.0)
    st.markdown("Published date range")
    published_from = st.date_input("Published from (optional)", value=None)
    published_to = st.date_input("Published to (optional)", value=None)
    # convert to pandas timestamps if provided
    published_from_ts = pd.to_datetime(published_from) if published_from else None
    published_to_ts = pd.to_datetime(published_to) if published_to else None

    page_size = st.selectbox("Page size", [10, 25, 50, 100], index=1)

# fetch & filter
df = fetch_listings(source)
st.markdown(f"**Total rows (source={source}):** {len(df):,}")

df_filtered = filter_df(
    df,
    property_type=prop_type_filter,
    city=city_filter,
    price_min=price_min,
    price_max=price_max,
    area_min=area_min,
    area_max=area_max,
    published_from=published_from_ts,
    published_to=published_to_ts,
)

# --- top metrics row ------------------------------------------------
col_a, col_b, col_c, col_d = st.columns(4)
col_a.metric("Listings (filtered)", f"{len(df_filtered):,}")
col_b.metric("Median PHP/sqm", f"{float(df_filtered['price_per_sqm'].median()):.0f}" if not df_filtered['price_per_sqm'].dropna().empty else "n/a")
col_c.metric("Avg PHP/sqm", f"{float(df_filtered['price_per_sqm'].mean()):.0f}" if not df_filtered['price_per_sqm'].dropna().empty else "n/a")
col_d.metric("Cities", df_filtered["city"].nunique())

st.markdown("---")

# --- main layout: table + charts -----------------------------------
left_col, right_col = st.columns((2, 3))

with left_col:
    st.subheader("Results")
    # show table (paginated by page_size)
    if df_filtered.empty:
        st.info("No rows match the current filters.")
    else:
        display_cols = ["listing_title", "city", "address", "property_type", "price_php", "area_sqm", "price_per_sqm", "published_at", "scraped_at", "url"]
        # ensure columns exist
        display_cols = [c for c in display_cols if c in df_filtered.columns]
        st.dataframe(df_filtered[display_cols].sort_values(by="published_at", ascending=False).head(page_size))

with right_col:
    st.subheader("Visualizations")

    # 1) Histogram / distribution of price_per_sqm
    fig_hist = px.histogram(
        df_filtered[df_filtered["price_per_sqm"].notnull()],
        x="price_per_sqm",
        nbins=30,
        title="Distribution: price_per_sqm (PHP/sqm)",
        labels={"price_per_sqm": "PHP / sqm"},
    )
    st.plotly_chart(fig_hist, use_container_width=True)

    # 2) Median price_per_sqm by city (bar)
    df_city = df_filtered.dropna(subset=["price_per_sqm", "city"]).groupby("city", as_index=False)["price_per_sqm"].median().sort_values("price_per_sqm", ascending=False)
    if not df_city.empty:
        fig_city = px.bar(df_city, x="city", y="price_per_sqm", title="Median PHP/sqm by city", labels={"price_per_sqm": "Median PHP/sqm"})
        st.plotly_chart(fig_city, use_container_width=True)

    # 3) Time series: listings count by month (published_at)
    if "published_at" in df_filtered.columns and df_filtered["published_at"].notnull().any():
        ts = df_filtered.dropna(subset=["published_at"]).copy()
        ts["month"] = ts["published_at"].dt.to_period("M").dt.to_timestamp()
        ts_count = ts.groupby("month").size().reset_index(name="count")
        fig_ts = px.line(ts_count, x="month", y="count", title="Listings over time (by published_at)")
        st.plotly_chart(fig_ts, use_container_width=True)
    else:
        st.info("No published_at timestamps available for time series chart.")

st.markdown("---")
st.caption("Data is pulled live from Supabase. Use 'Refresh data' in the sidebar to clear app cache.")


