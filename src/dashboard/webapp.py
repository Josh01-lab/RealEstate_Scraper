import sys
from pathlib import Path

# webapp.py -> src/dashboard/webapp.py
# repo root is two levels up from src: <repo>/
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
# ---------------------------------------------------

import os
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
import plotly.express as px

from src.db.supabase_client import get_client

st.set_page_config(
    page_title="Cebu Office Listings — Analytics",
    layout="wide",
    page_icon="🏢",
)

try:
    import plotly.express as px
    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False

# later when plotting:
if HAS_PLOTLY:
    fig = px.bar(df, x="something", y="value")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Plotly not installed; using fallback chart.")
    st.bar_chart(df.set_index("something")["value"])

# -------------------- Data --------------------
@st.cache_data(ttl=600)
def load_listings(source: str | None):
    sb = get_client()
    q = sb.table("listings").select(
        "url,listing_title,address,property_type,price_php,area_sqm,price_per_sqm,"
        "city,published_at,published_at_text,scraped_at,source"
    )
    if source:
        q = q.eq("source", source)

    rows = q.limit(5000).execute().data or []
    df = pd.DataFrame(rows)

    # Make sure numeric columns are numeric
    for col in ["price_php", "area_sqm", "price_per_sqm"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Create a single "date" column (prefer published_at, then scraped_at)
    def coalesce_dt(row):
        for c in ("published_at", "scraped_at"):
            v = row.get(c)
            if pd.notna(v):
                try:
                    return pd.to_datetime(v, utc=True)
                except Exception:
                    pass
        return pd.NaT

    if not df.empty:
        df["date"] = df.apply(coalesce_dt, axis=1)
        # derive a display city if missing
        if "city" not in df.columns or df["city"].isna().all():
            # try to guess city from address text
            def guess_city(addr):
                if not isinstance(addr, str):
                    return None
                for c in ["Cebu City", "Cebu", "Mandaue", "Lapu-Lapu"]:
                    if c.lower() in addr.lower():
                        return c
                return None
            df["city"] = df.get("city") if "city" in df.columns else None
            if "city" not in df or df["city"].isna().any():
                df["city"] = df.apply(lambda r: r.get("city") or guess_city(r.get("address")), axis=1)

    return df


# -------------------- Sidebar Filters --------------------
st.sidebar.header("Data Source & Cache")
source = st.sidebar.text_input("Source tag (eq listings.source)", value="lamudi_cebu")
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()

df = load_listings(source or None)

st.sidebar.header("Filters")

# City multiselect
city_options = sorted([c for c in df["city"].dropna().unique().tolist()]) if not df.empty else []
cities = st.sidebar.multiselect("City", city_options, default=city_options[:1] if city_options else [])

# Property type
ptype_options = sorted([p for p in df["property_type"].dropna().unique().tolist()]) if not df.empty else []
ptypes = st.sidebar.multiselect("Property type", ptype_options, default=["Offices"] if "Offices" in ptype_options else ptype_options[:1])

# Price per sqm range
pps_min = float(df["price_per_sqm"].min()) if not df.empty and pd.notna(df["price_per_sqm"].min()) else 0.0
pps_max = float(df["price_per_sqm"].max()) if not df.empty and pd.notna(df["price_per_sqm"].max()) else 1000.0
pps_range = st.sidebar.slider("Price per sqm (PHP)", min_value=0.0, max_value=max(pps_max, 1.0),
                              value=(max(pps_min, 0.0), max(pps_max, 1.0)), step=1.0)

# Date range (published/scraped)
if not df.empty and df["date"].notna().any():
    dmin = pd.to_datetime(df["date"].min()).date()
    dmax = pd.to_datetime(df["date"].max()).date()
else:
    today = datetime.now(timezone.utc).date()
    dmin = today - timedelta(days=365)
    dmax = today

drange = st.sidebar.date_input("Date range (published/scraped)", (dmin, dmax))

# Search text
search_q = st.sidebar.text_input("Search text (title/address)")

# -------------------- Apply filters --------------------
df_f = df.copy()

if cities:
    df_f = df_f[df_f["city"].isin(cities)]

if ptypes:
    df_f = df_f[df_f["property_type"].isin(ptypes)]

df_f = df_f[(df_f["price_per_sqm"].fillna(-1) >= pps_range[0]) &
            (df_f["price_per_sqm"].fillna(-1) <= pps_range[1])]

if isinstance(drange, (list, tuple)) and len(drange) == 2:
    start, end = drange
    # include entire end day
    end_ts = pd.to_datetime(datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc))
    start_ts = pd.to_datetime(datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc))
    df_f = df_f[(df_f["date"] >= start_ts) & (df_f["date"] <= end_ts)]

if search_q:
    s = search_q.lower()
    df_f = df_f[df_f.apply(
        lambda r: (isinstance(r.get("listing_title"), str) and s in r["listing_title"].lower()) or
                  (isinstance(r.get("address"), str) and s in r["address"].lower()),
        axis=1
    )]

# -------------------- Header + KPIs --------------------
st.title("🏢 Cebu Office Listings — Analytics")

count_filtered = int(len(df_f))
median_pps = float(df_f["price_per_sqm"].median()) if not df_f.empty else 0.0
mean_pps = float(df_f["price_per_sqm"].mean()) if not df_f.empty else 0.0
n_cities = int(df_f["city"].nunique()) if not df_f.empty else 0

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Listings (filtered)", f"{count_filtered:,}")
kpi2.metric("Median PHP/sqm", f"₱ {median_pps:,.0f}")
kpi3.metric("Avg PHP/sqm", f"₱ {mean_pps:,.0f}")
st.caption(f"Cities: {n_cities}")

# -------------------- Results table --------------------
st.subheader("Results")
show_cols = ["listing_title", "city", "address", "property_type", "price_php", "area_sqm", "price_per_sqm", "date", "url"]
present_cols = [c for c in show_cols if c in df_f.columns]
st.dataframe(
    df_f[present_cols].sort_values(by=["price_per_sqm", "price_php"], ascending=[True, True]),
    use_container_width=True,
    hide_index=True
)

# -------------------- Distribution chart --------------------
st.subheader("Distribution of Price per sqm")
if df_f["price_per_sqm"].notna().sum() > 0:
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



