import sys, os
from pathlib import Path


# Ensure repo root on sys.path (one level above src/)
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta, timezone


# Supabase client from your project
from src.db.supabase_client import get_client # <-- important


st.set_page_config(page_title="Listings Dashboard", layout="wide")


@st.cache_data(ttl=300)
def load_data(source_tag: str | None = None):
    sb = get_client()
    q = sb.table("listings").select("*")
    if source_tag:
        q = q.eq("source", source_tag)
    return q.order("scraped_at", desc=True).limit(1000).execute().data


def prep_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    df = pd.DataFrame(df).copy()


    # numeric columns
    for col in ["price_php", "area_sqm", "price_per_sqm"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


    # coalesced datetime (prefer published_at)
    def coalesce_dt(row):
        for c in ("published_at", "scraped_at"):
            v = row.get(c)
            if pd.notna(v):
                try:
                    return pd.to_datetime(v, utc=True)
                except Exception:
                    pass
        return pd.NaT


    df["date"] = df.apply(coalesce_dt, axis=1)


    # derive city if missing or empty
    if ("city" not in df.columns) or df["city"].isna().all():
        def guess_city(addr):
            if not isinstance(addr, str):
                return None
            for c in ["Cebu City", "Cebu", "Mandaue", "Lapu-Lapu"]:
                if c.lower() in addr.lower():
                    return c
            return None
        df["city"] = df["city"] if "city" in df.columns else None
        df["city"] = df.apply(lambda r: r.get("city") or guess_city(r.get("address")), axis=1)


    return df


# -------------------- Sidebar / controls --------------------
st.sidebar.header("Data Source & Cache")
source_tag = st.sidebar.text_input("Filter by source (listings.source)", value="lamudi_cebu")
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()


# Load + prep
df = pd.DataFrame(load_data(source_tag or None))
df = prep_dataframe(df)


if df.empty:
    st.warning("No data available yet. Check your Supabase credentials or run the scraper to publish rows.")
    st.stop()


st.title("Cebu Office Listings — Analytics")


# Filters
st.sidebar.header("Filters")
city_options = sorted(df["city"].dropna().unique().tolist()) if "city" in df.columns else []
cities = st.sidebar.multiselect("City", city_options, default=(city_options[:1] if city_options else []))


ptype_options = sorted(df["property_type"].dropna().unique().tolist()) if "property_type" in df.columns else []
ptypes = st.sidebar.multiselect(
    "Property type",
    ptype_options,
    default=(["Offices"] if "Offices" in ptype_options else (ptype_options[:1] if ptype_options else []))
)


if "price_per_sqm" in df.columns and pd.notna(df["price_per_sqm"].min()):
    pps_min = float(df["price_per_sqm"].min())
    pps_max = float(df["price_per_sqm"].max())
else:
    pps_min, pps_max = 0.0, 1000.0


pps_range = st.sidebar.slider(
    "Price per sqm (PHP)",
    0.0, max(pps_max, 1.0),
    (max(pps_min, 0.0), max(pps_max, 1.0)),
    step=1.0
)


if df["date"].notna().any():
    dmin = pd.to_datetime(df["date"].min()).date()
    dmax = pd.to_datetime(df["date"].max()).date()
else:
    today = datetime.now(timezone.utc).date()
    dmin, dmax = today - timedelta(days=365), today


drange = st.sidebar.date_input("Date range (published/scraped)", (dmin, dmax))
search_q = st.sidebar.text_input("Search text (title/address)")


# Apply filters
df_f = df.copy()
if cities:
    df_f = df_f[df_f["city"].isin(cities)]
if ptypes:
    df_f = df_f[df_f["property_type"].isin(ptypes)]
if "price_per_sqm" in df_f.columns:
    df_f = df_f[
        (df_f["price_per_sqm"].fillna(-1) >= pps_range[0]) &
        (df_f["price_per_sqm"].fillna(-1) <= pps_range[1])
    ]
if isinstance(drange, (list, tuple)) and len(drange) == 2:
    start, end = drange
    start_ts = pd.to_datetime(datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc))
    end_ts = pd.to_datetime(datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc))
    df_f = df_f[(df_f["date"] >= start_ts) & (df_f["date"] <= end_ts)]
if search_q:
    s = search_q.lower()
    df_f = df_f[df_f.apply(
        lambda r: (isinstance(r.get("listing_title"), str) and s in r["listing_title"].lower()) or
                  (isinstance(r.get("address"), str) and s in r["address"].lower()),
        axis=1
    )]


# KPIs
count_filtered = int(len(df_f))
median_pps = float(df_f["price_per_sqm"].median()) if ("price_per_sqm" in df_f.columns and not df_f.empty) else 0.0
mean_pps = float(df_f["price_per_sqm"].mean()) if ("price_per_sqm" in df_f.columns and not df_f.empty) else 0.0
n_cities = int(df_f["city"].nunique()) if "city" in df_f.columns else 0


k1, k2, k3 = st.columns(3)
k1.metric("Listings (filtered)", f"{count_filtered:,}")
k2.metric("Median PHP/sqm", f"₱ {median_pps:,.0f}")
k3.metric("Avg PHP/sqm", f"₱ {mean_pps:,.0f}")
st.caption(f"Cities: {n_cities}")


# Chart
if ("property_type" in df_f.columns) and ("price_php" in df_f.columns):
    agg = (
        df_f.dropna(subset=["property_type", "price_php"])
            .groupby("property_type", as_index=False)["price_php"].median()
            .rename(columns={"price_php": "median_price_php"})
    )
    if not agg.empty:
        fig = px.bar(agg, x="property_type", y="median_price_php", title="Median Price by Property Type")
        st.plotly_chart(fig, use_container_width=True)


# Table
st.subheader("Results")
show_cols = ["listing_title", "city", "address", "property_type", "price_php", "area_sqm", "price_per_sqm", "date", "url"]
present_cols = [c for c in show_cols if c in df_f.columns]
st.dataframe(
    df_f[present_cols].sort_values(
        by=[c for c in ["price_per_sqm", "price_php"] if c in df_f.columns],
        ascending=[True, True]
    ),
    use_container_width=True,
    hide_index=True
)


# Distribution
st.subheader("Distribution of Price per sqm")
if ("price_per_sqm" in df_f.columns) and (df_f["price_per_sqm"].notna().sum() > 0):
    fig = px.histogram(df_f, x="price_per_sqm", nbins=20, opacity=0.9)
    fig.update_layout(
        xaxis_title="PHP per sqm",
        yaxis_title="Listings",
        bargap=0.05,
        height=420,
        margin=dict(l=10, r=10, t=30, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No price_per_sqm values to plot.")

