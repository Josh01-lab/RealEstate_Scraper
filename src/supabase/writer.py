from _future_ import annotations
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from .client import get_supabase

def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def upsert_listing_by_url(rec: Dict[str, Any]) -> str:
    """
    Calls the SQL function public.upsert_listing_by_url in Supabase.
    rec keys (normalized): url, listing_title, property_type, address,
                           price_php, area_sqm, price_per_sqm,
                           price_json, area_json, scraped_at, source
    Returns listing_id (uuid).
    """
    sb = get_supabase()
    payload = {
        "p_url":            rec.get("url"),
        "p_listing_title":  rec.get("listing_title"),
        "p_property_type":  rec.get("property_type"),
        "p_address":        rec.get("address"),
        "p_price_php":      rec.get("price_php"),
        "p_area_sqm":       rec.get("area_sqm"),
        "p_price_per_sqm":  rec.get("price_per_sqm"),
        "p_price_json":     rec.get("price_json"),
        "p_area_json":      rec.get("area_json"),
        "p_scraped_at":     _iso(rec.get("scraped_at")),
        "p_source":         rec.get("source"),
    }
    # RPC returns a scalar uuid (string)
    resp = sb.rpc("upsert_listing_by_url", payload).execute()
    listing_id = resp.data  # supabase-py v2 returns parsed JSON; RPC returns the uuid value
    if not listing_id:
        raise RuntimeError(f"RPC upsert_listing_by_url returned empty: {resp}")
    return listing_id

def insert_daily_snapshot(listing_id: str, snap: Dict[str, Any]) -> None:
    """
    Calls public.insert_listing_daily_snapshot
    snap keys: seen_at (datetime), price_php, area_sqm, price_per_sqm, is_active, property_type, source
    """
    sb = get_supabase()
    payload = {
        "p_listing_id":     listing_id,
        "p_seen_at":        _iso(snap.get("seen_at")),
        "p_price_php":      snap.get("price_php"),
        "p_area_sqm":       snap.get("area_sqm"),
        "p_price_per_sqm":  snap.get("price_per_sqm"),
        "p_is_active":      snap.get("is_active", True),
        "p_property_type":  snap.get("property_type"),
        "p_source":         snap.get("source"),
    }
    sb.rpc("insert_listing_daily_snapshot", payload).execute()
