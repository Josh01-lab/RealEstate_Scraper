import os
import io
import csv
from datetime import datetime, timezone, date
from typing import Dict, Any, List, Optional, Tuple

import streamlit as st
import pandas as pd

# Ensure your project imports work: this expects your package layout where
# src/db/supabase_client.py defines get_client()
try:
    from src.db.supabase_client import get_client
except Exception as e:
    st.error("Could not import get_client from src.db.supabase_client. "
             "Make sure your PYTHONPATH includes the project root and the file exists.\n"
             f"Import error: {e}")
    raise

# ---------------------------
# Helpers: build & run query
# ---------------------------
COLUMNS = [
    "id", "url", "listing_title", "address", "property_type",
    "price_php", "area_sqm", "price_per_sqm",
    "published_at", "published_at_text", "scraped_at", "source"
]


def safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _apply_filters(builder, filters: Dict[str, Any]):
    """
    Apply filters to a Postgrest / supabase query builder.
    We always return the builder (chained).
    """
    # Text search
    q_text = filters.get("q_text")
    if q_text:
        # ilike supports % wildcards
        pat = f"%{q_text}%"
        # apply to both listing_title and address using OR is not supported in simple chain;
        # We do two queries and combine counts if needed — but simplest: ilike on listing_title OR address
        # PostgREST doesn't support OR easily; instead we'll use ilike on listing_title and fallback to address if nothing
        # To keep it simple and reliable, we'll apply ilike to listing_title and address separately later.
        # For now apply listing_title ilike
        builder = builder.ilike("listing_title", pat)

    # Source
    source = filters.get("source")
    if source:
        builder = builder.eq("source", source)

    # Property Type
    ptype = filters.get("property_type")
    if ptype:
        # assume exact match
        builder = builder.eq("property_type", ptype)

    # Price range
    min_price = safe_float(filters.get("min_price"))
    max_price = safe_float(filters.get("max_price"))
    if min_price is not None:
        builder = builder.gte("price_php", min_price)
    if max_price is not None:
        builder = builder.lte("price_php", max_price)

    # Area range
    min_area = safe_float(filters.get("min_area"))
    max_area = safe_float(filters.get("max_area"))
    if min_area is not None:
        builder = builder.gte("area_sqm", min_area)
    if max_area is not None:
        builder = builder.lte("area_sqm", max_area)

    # Date filters (published_at or scraped_at)
    # We expect (start_date, end_date) as date objects or None
    pub_start = filters.get("published_start")
    pub_end = filters.get("published_end")
    if pub_start:
        iso_start = datetime.combine(pub_start, datetime.min.time()).astimezone(timezone.utc).isoformat()
        builder = builder.gte("published_at", iso_start)
    if pub_end:
        iso_end = datetime.combine(pub_end, datetime.max.time()).astimezone(timezone.utc).isoformat()
        builder = builder.lte("published_at", iso_end)

    scraped_start = filters.get("scraped_start")
    scraped_end = filters.get("scraped_end")
    if scraped_start:
        iso_start = datetime.combine(scraped_start, datetime.min.time()).astimezone(timezone.utc).isoformat()
        builder = builder.gte("scraped_at", iso_start)
    if scraped_end:
        iso_end = datetime.combine(scraped_end, datetime.max.time()).astimezone(timezone.utc).isoformat()
        builder = builder.lte("scraped_at", iso_end)

    return builder


@st.cache_data(ttl=60)
def fetch_property_types(limit=500) -> List[str]:
    """Fetch distinct property_type values (small cache)."""
    sb = get_client()
    try:
        resp = sb.table("listings").select("property_type", count="exact").limit(limit).execute()
        rows = resp.data or []
        s = sorted({r.get("property_type") for r in rows if r.get("property_type")})
        return s
    except Exception:
        return []


@st.cache_data(ttl=15)
def fetch_listings(filters: Dict[str, Any], page: int = 1, per_page: int = 25) -> Tuple[List[Dict[str, Any]], int]:
    """
    Build and execute the Supabase query with filters, pagination.
    Returns (rows, total_count).
    Note: PostgREST supports returning count with count='exact'. We do one query for count+page content.
    """
    sb = get_client()
    # select only columns that exist in DB; defensive: request COLUMNS and fallback to "*"
    cols = ",".join(COLUMNS)
    builder = sb.table("listings").select(cols, count="exact")

    # Apply filters
    builder = _apply_filters(builder, filters)

    # Ordering & pagination
    # We order by scraped_at desc (if present), else fallback to published_at desc
    try:
        builder = builder.order("scraped_at", desc=True)
    except Exception:
        try:
            builder = builder.order("published_at", desc=True)
        except Exception:
            pass

    offset_val = (page - 1) * per_page
    try:
        resp = builder.limit(per_page).offset(offset_val).execute()
    except Exception as e:
        # better error feedback than crash
        st.error(f"Error querying Supabase: {e}")
        return [], 0

    if resp is None:
        return [], 0

    rows = resp.data or []
    total = getattr(resp, "count", None)
    # if count wasn't provided by the SDK, try to fetch separately (cheap fallback)
    if total is None:
        try:
            total_resp = sb.table("listings").select("id", count="exact").execute()
            total = total_resp.count or len(rows)
        except Exception:
            total = len(rows)

    # If the user typed a free-text query, apply address ilike fallback merge
    q_text = filters.get("q_text")
    if q_text and rows:
        # If results are low, try to supplement by searching address too and merging unique urls
        if len(rows) < per_page:
            pat = f"%{q_text}%"
            try:
                add_resp = sb.table("listings").select(cols).ilike("address", pat).limit(per_page).execute()
                add_rows = add_resp.data or []
                # merge avoiding duplicates by URL
                existing_urls = {r.get("url") for r in rows}
                for r in add_rows:
                    if r.get("url") not in existing_urls:
                        rows.append(r)
            except Exception:
                pass

    return rows, int(total or 0)


# ---------------------------
# Streamlit UI
# ---------------------------
st.set_page_config(page_title="Listings Explorer", layout="wide")
st.title("Listings Explorer — Supabase")

with st.sidebar:
    st.header("Filters")

    # text search
    q_text = st.text_input("Search (title/address)", placeholder="office, Ayala, BPO ...")

    # source selector - fetch distinct sources from DB (quick sample)
    sb = get_client()
    try:
        src_resp = sb.table("listings").select("source", count="exact").limit(100).execute()
        available_sources = sorted({r.get("source") for r in (src_resp.data or []) if r.get("source")})
    except Exception:
        available_sources = []
    source = st.selectbox("Source", [""] + available_sources, index=0)

    # property type
    prop_types = fetch_property_types()
    prop_types_opt = [""] + prop_types
    property_type = st.selectbox("Property type", prop_types_opt, index=0)

    # price and area ranges
    col1, col2 = st.columns(2)
    with col1:
        min_price = st.number_input("Min price (PHP)", min_value=0.0, value=0.0, step=100.0, format="%f")
        max_price = st.number_input("Max price (PHP, 0 = no limit)", min_value=0.0, value=0.0, step=100.0, format="%f")
    with col2:
        min_area = st.number_input("Min area (sqm)", min_value=0.0, value=0.0, step=1.0)
        max_area = st.number_input("Max area (sqm, 0 = no limit)", min_value=0.0, value=0.0, step=1.0)

    # published_at date range
    st.markdown("**Published date range**")
    published_start = st.date_input("Published from", value=None)
    published_end = st.date_input("Published to", value=None)

    # scraped_at date range
    st.markdown("**Scraped date range**")
    scraped_start = st.date_input("Scraped from", value=None, key="scraped_from")
    scraped_end = st.date_input("Scraped to", value=None, key="scraped_to")

    # pagination & page size
    per_page = st.selectbox("Page size", [10, 25, 50, 100], index=1)

    st.markdown("---")
    if st.button("Apply filters"):
        st.experimental_rerun()

# Collect filters into dict
filters = {
    "q_text": q_text.strip() or None,
    "source": source or None,
    "property_type": property_type or None,
    "min_price": None if min_price == 0 else min_price,
    "max_price": None if max_price == 0 else max_price,
    "min_area": None if min_area == 0 else min_area,
    "max_area": None if max_area == 0 else max_area,
    "published_start": published_start if isinstance(published_start, date) else None,
    "published_end": published_end if isinstance(published_end, date) else None,
    "scraped_start": scraped_start if isinstance(scraped_start, date) else None,
    "scraped_end": scraped_end if isinstance(scraped_end, date) else None,
}

# Pagination state
if "page" not in st.session_state:
    st.session_state.page = 1

colp1, colp2, colp3 = st.columns([1, 1, 6])
with colp1:
    if st.button("Prev") and st.session_state.page > 1:
        st.session_state.page -= 1
with colp2:
    if st.button("Next"):
        st.session_state.page += 1
with colp3:
    st.write(f"Page {st.session_state.page}")

# Fetch listings (cached)
rows, total = fetch_listings(filters, page=st.session_state.page, per_page=per_page)

st.markdown(f"**Total matching (approx):** {total} — Showing {len(rows)} rows on this page")

# Convert to DataFrame
if rows:
    df = pd.DataFrame(rows)
    # normalize common columns if missing
    for c in ["price_php", "area_sqm", "price_per_sqm", "published_at", "description", "published_at_text"]:
        if c not in df.columns:
            df[c] = None

    # prettify timestamps
    if "published_at" in df.columns:
        try:
            df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
        except Exception:
            pass
    if "scraped_at" in df.columns:
        try:
            df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True, errors="coerce")
        except Exception:
            pass

    st.dataframe(df[["listing_title", "address", "property_type", "price_php", "area_sqm", "price_per_sqm", "published_at", "scraped_at"]].fillna(""))

    # Row expansion
    def _show_row(idx):
        r = rows[idx]
        st.markdown(f"### {r.get('listing_title') or r.get('url')}")
        st.write("**URL:**", r.get("url"))
        st.write("**Address:**", r.get("address"))
        st.write("**Type:**", r.get("property_type"))
        st.write("**Price:**", r.get("price_php"))
        st.write("**Area (sqm):**", r.get("area_sqm"))
        st.write("**Published at:**", r.get("published_at") or r.get("published_at_text"))
        st.markdown("**Description**")
        st.write(r.get("description") or "_(no description)_")

    for i, _ in enumerate(rows):
        with st.expander(f"{i+1}. {rows[i].get('listing_title') or rows[i].get('url')}"):
            _show_row(i)

    # Export CSV for current page
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    csv_bytes = csv_buf.getvalue().encode("utf-8")
    st.download_button("Export page CSV", data=csv_bytes, file_name="listings_page.csv", mime="text/csv")
else:
    st.info("No rows returned with current filters. Try removing some filters or widening date/price ranges.")

# Footer: quick tips
st.markdown("---")
st.markdown(
    """
    **Notes**
    - The UI filters `published_at` and `scraped_at` expect ISO timestamps in the DB.
    - If `published_at` is null for a listing it may not be present on the source page (or parsing failed).
    - Filters are applied server-side; if you see unexpected results double-check column types in Supabase.
    """
)

