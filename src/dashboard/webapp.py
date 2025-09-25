import os
import math
from datetime import datetime, timedelta, timezone, date

import pandas as pd
import streamlit as st

# If you already have supabase client helpers in your repo, prefer importing them:
#   from src.db.supabase_client import get_client
# Otherwise use the official client below.
try:
    from src.db.supabase_client import get_client as _get_client  # your existing helper
    def get_client():
        return _get_client()
except Exception:
    # Fallback to official client (make sure it's in requirements.txt)
    from supabase import create_client, Client  # type: ignore

    def get_client() -> "Client":
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_ANON_KEY")
        if not url or not key:
            raise RuntimeError(
                "Missing SUPABASE_URL / SUPABASE_ANON_KEY. "
                "Set them as environment variables."
            )
        return create_client(url, key)

TABLE = os.getenv("LISTINGS_TABLE", "listings")

st.set_page_config(
    page_title="Real Estate Listings",
    page_icon="",
    layout="wide",
)

# ------------------------ Utilities ------------------------ #

@st.cache_data(show_spinner=False, ttl=300)
def fetch_sources() -> list[str]:
    sb = get_client()
    try:
        rows = (
            sb.table(TABLE)
              .select("source")
              .neq("source", None)
              .execute()
              .data
        )
        return sorted({r["source"] for r in rows if r.get("source")})
    except Exception:
        return []

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

@st.cache_data(show_spinner=True, ttl=60)
def query_listings(
    sources: list[str],
    q_address: str | None,
    q_title: str | None,
    prop_types: list[str],
    price_min: float | None,
    price_max: float | None,
    area_min: float | None,
    area_max: float | None,
    scraped_from: date | None,
    scraped_to: date | None,
    published_from: date | None,
    published_to: date | None,
    page: int,
    page_size: int,
    order_by: str,
    order_desc: bool,
) -> tuple[pd.DataFrame, int]:
    sb = get_client()
    rq = sb.table(TABLE).select(
        "url,listing_title,address,property_type,price_php,area_sqm,price_per_sqm,published_at,scraped_at,source"
    , count="exact")

    # Filters
    if sources:
        # Supabase: use 'in' with a list
        rq = rq.in_("source", sources)
    if q_address:
        rq = rq.ilike("address", f"%{q_address}%")
    if q_title:
        rq = rq.ilike("listing_title", f"%{q_title}%")
    if prop_types:
        rq = rq.in_("property_type", prop_types)
    if price_min is not None:
        rq = rq.gte("price_php", price_min)
    if price_max is not None:
        rq = rq.lte("price_php", price_max)
    if area_min is not None:
        rq = rq.gte("area_sqm", area_min)
    if area_max is not None:
        rq = rq.lte("area_sqm", area_max)
    if scraped_from:
        rq = rq.gte("scraped_at", datetime.combine(scraped_from, datetime.min.time()).isoformat() + "Z")
    if scraped_to:
        # inclusive to end of day
        end = datetime.combine(scraped_to, datetime.max.time()).replace(microsecond=0).isoformat() + "Z"
        rq = rq.lte("scraped_at", end)
    if published_from:
        rq = rq.gte("published_at", datetime.combine(published_from, datetime.min.time()).isoformat() + "Z")
    if published_to:
        end = datetime.combine(published_to, datetime.max.time()).replace(microsecond=0).isoformat() + "Z"
        rq = rq.lte("published_at", end)

    # Sort
    rq = rq.order(order_by, desc=order_desc, nullsfirst=False)

    # Pagination
    start = page * page_size
    end = start + page_size - 1
    rq = rq.range(start, end)

    res = rq.execute()
    rows = res.data or []
    total = res.count or 0

    # Normalize to DataFrame
    df = pd.DataFrame(rows)
    if not df.empty:
        # Ensure numeric dtype
        for col in ["price_php", "area_sqm", "price_per_sqm"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        # Compute price_per_sqm if missing
        missing_pps = df["price_per_sqm"].isna() & df["price_php"].notna() & df["area_sqm"].notna() & (df["area_sqm"] > 0)
        df.loc[missing_pps, "price_per_sqm"] = df.loc[missing_pps, "price_php"] / df.loc[missing_pps, "area_sqm"]

        # Parse datetimes
        for col in ["published_at", "scraped_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df, int(total)

@st.cache_data(show_spinner=False, ttl=120)
def top_kpi(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"count": 0, "avg_pps": None, "median_price": None}
    return {
        "count": len(df),
        "avg_pps": float(df["price_per_sqm"].dropna().mean()) if "price_per_sqm" in df else None,
        "median_price": float(df["price_php"].dropna().median()) if "price_php" in df else None,
    }

def make_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


# ------------------------ Sidebar Filters ------------------------ #
st.sidebar.header("Filters")

all_sources = fetch_sources()
src_sel = st.sidebar.multiselect("Source", all_sources, default=all_sources[:1] if all_sources else [])

col1, col2 = st.sidebar.columns(2)
with col1:
    price_min = st.number_input("Min Price (PHP)", min_value=0, value=0, step=1000, format="%d")
with col2:
    price_max = st.number_input("Max Price (PHP)", min_value=0, value=0, step=1000, format="%d")
price_min = price_min or None
price_max = price_max or None

col3, col4 = st.sidebar.columns(2)
with col3:
    area_min = st.number_input("Min Area (sqm)", min_value=0, value=0, step=1, format="%d")
with col4:
    area_max = st.number_input("Max Area (sqm)", min_value=0, value=0, step=1, format="%d")
area_min = area_min or None
area_max = area_max or None

addr_q = st.sidebar.text_input("Search Address contains…")
title_q = st.sidebar.text_input("Search Title contains…")

today = date.today()
scr_from, scr_to = st.sidebar.date_input(
    "Scraped date range",
    value=(today - timedelta(days=7), today),
)
pub_from, pub_to = st.sidebar.date_input(
    "Published date range (optional)",
    value=(None, None),
)

# property types (pull from Supabase quickly via a small sample)
@st.cache_data(show_spinner=False, ttl=300)
def fetch_property_types() -> list[str]:
    sb = get_client()
    try:
        rows = (
            sb.table(TABLE)
              .select("property_type")
              .neq("property_type", None)
              .limit(1000)
              .execute()
              .data
        )
        return sorted({r["property_type"] for r in rows if r.get("property_type")})
    except Exception:
        return []

ptype_opts = fetch_property_types()
ptype_sel = st.sidebar.multiselect("Property Type", ptype_opts, default=[])

# Sorting & pagination
st.sidebar.header("View")
order_by = st.sidebar.selectbox(
    "Sort by",
    options=["scraped_at", "published_at", "price_php", "price_per_sqm", "area_sqm", "listing_title"],
    index=0,
)
order_desc = st.sidebar.toggle("Descending", value=True)
page_size = st.sidebar.selectbox("Rows per page", options=[25, 50, 100], index=0)

# ------------------------ Query & Results ------------------------ #
page = st.session_state.get("page", 0)
# reset page on any filter change by keying state
if "prev_filters" not in st.session_state:
    st.session_state.prev_filters = None
current_filters = (tuple(src_sel), addr_q, title_q, tuple(ptype_sel),
                   price_min, price_max, area_min, area_max, scr_from, scr_to, pub_from, pub_to, order_by, order_desc, page_size)
if st.session_state.prev_filters != current_filters:
    page = 0
st.session_state.prev_filters = current_filters

df, total = query_listings(
    sources=src_sel,
    q_address=addr_q or None,
    q_title=title_q or None,
    prop_types=ptype_sel,
    price_min=_safe_float(price_min),
    price_max=_safe_float(price_max),
    area_min=_safe_float(area_min),
    area_max=_safe_float(area_max),
    scraped_from=scr_from if isinstance(scr_from, date) else None,
    scraped_to=scr_to if isinstance(scr_to, date) else None,
    published_from=pub_from if isinstance(pub_from, date) else None,
    published_to=pub_to if isinstance(pub_to, date) else None,
    page=page,
    page_size=page_size,
    order_by=order_by,
    order_desc=order_desc,
)

# KPIs
kpis = top_kpi(df)
k1, k2, k3 = st.columns(3)
k1.metric("Listings (page)", f"{len(df):,}")
k2.metric("Avg Price / sqm", f"₱{kpis['avg_pps']:,.0f}" if kpis["avg_pps"] else "—")
k3.metric("Median Price", f"₱{kpis['median_price']:,.0f}" if kpis["median_price"] else "—")

# Charts (simple & fast)
if not df.empty:
    c1, c2 = st.columns(2)
    with c1:
        st.write("**Count by scraped date**")
        tmp = df.copy()
        tmp["scraped_date"] = pd.to_datetime(tmp["scraped_at"]).dt.date
        chart = (
            tmp.groupby("scraped_date")["url"].count()
            .rename("count")
            .reset_index()
        )
        st.bar_chart(chart, x="scraped_date", y="count")
    with c2:
        st.write("**Avg price/sqm by property type**")
        tmp = df.copy()
        if "property_type" in tmp.columns:
            chart2 = (
                tmp.groupby("property_type")["price_per_sqm"]
                .mean()
                .fillna(0)
                .rename("avg_price_per_sqm")
                .reset_index()
            )
            st.bar_chart(chart2, x="property_type", y="avg_price_per_sqm")

# Data table
st.write("### Listings")
if df.empty:
    st.info("No results for the current filters.")
else:
    # Make URL clickable in Streamlit dataframe
    show = df.copy()
    show["price_php"] = show["price_php"].map(lambda v: f"₱{v:,.0f}" if pd.notna(v) else "")
    show["price_per_sqm"] = show["price_per_sqm"].map(lambda v: f"₱{v:,.0f}" if pd.notna(v) else "")
    show["area_sqm"] = show["area_sqm"].map(lambda v: f"{v:,.0f} sqm" if pd.notna(v) else "")
    show["scraped_at"] = show["scraped_at"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M UTC") if pd.api.types.is_datetime64tz_dtype(show["scraped_at"]) else show["scraped_at"]
    if "published_at" in show.columns and pd.api.types.is_datetime64tz_dtype(show["published_at"]):
        show["published_at"] = show["published_at"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d")

    # Move URL earlier & make it markdown
    show.insert(0, "link", show["url"].map(lambda u: f"[open]({u})" if isinstance(u, str) else ""))
    show = show.drop(columns=["url"])

    st.dataframe(
        show,
        use_container_width=True,
        hide_index=True,
    )

# Pagination controls
total_pages = math.ceil(total / page_size) if page_size else 1
colp1, colp2, colp3 = st.columns([1, 2, 1])
with colp1:
    if st.button(" Prev", disabled=(page <= 0)):
        page = max(0, page - 1)
with colp2:
    st.write(f"Page {page + 1} of {max(total_pages,1)} — {total:,} total")
with colp3:
    if st.button("Next", disabled=(page + 1 >= total_pages)):
        page = min(total_pages - 1, page + 1)
st.session_state.page = page

# Export
st.download_button(
    "Download CSV",
    data=make_csv(df),
    file_name=f"listings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)


