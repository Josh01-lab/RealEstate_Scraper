from typing import Dict, Any, Tuple, List
from src.db.supabase_client import get_client

def _apply_filters(tbl, filters: Dict[str, Any]):
    """Apply only provided filters to a PostgREST query builder (supabase-py style)."""
    if not filters:
        return tbl
    # equality filters
    if filters.get("source"):
        tbl = tbl.eq("source", filters["source"])
    if filters.get("property_type"):
        tbl = tbl.eq("property_type", filters["property_type"])
    # range filters (price)
    if filters.get("min_price") is not None:
        tbl = tbl.gte("price_php", filters["min_price"])
    if filters.get("max_price") is not None:
        tbl = tbl.lte("price_php", filters["max_price"])
    # range filters (area)
    if filters.get("min_area") is not None:
        tbl = tbl.gte("area_sqm", filters["min_area"])
    if filters.get("max_area") is not None:
        tbl = tbl.lte("area_sqm", filters["max_area"])
    # published_at range
    if filters.get("published_from"):
        tbl = tbl.gte("published_at", filters["published_from"])
    if filters.get("published_to"):
        tbl = tbl.lte("published_at", filters["published_to"])
    # text search (optional)
    if filters.get("q"):
        # simple ILIKE against listing_title and description (may be slow; consider full-text)
        q = f"%{filters['q']}%"
        tbl = tbl.or_(f"listing_title.ilike.{q},description.ilike.{q}")
    return tbl

def fetch_listings_and_count(filters: Dict[str, Any], page: int = 1, per_page: int = 25) -> Tuple[int, List[Dict]]:
    sb = get_client()
    tbl = sb.table("listings")
    # apply filters only if any present
    if filters:
        tbl = _apply_filters(tbl, filters)

    # count (exact)
    count_resp = tbl.select("*", count="exact").execute()
    total = count_resp.count if hasattr(count_resp, "count") else (count_resp.get("count") if isinstance(count_resp, dict) else None)

    # page of rows (ordering by scraped_at desc)
    offset = (page - 1) * per_page
    rows = tbl.select("id,url,listing_title,price_php,area_sqm,price_per_sqm,address,property_type,published_at,scraped_at") \
              .order("scraped_at", desc=True) \
              .range(offset, offset + per_page - 1) \
              .execute().data

    return int(total or 0), rows
