# src/metrics/market_metrics.py
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

DBURL = os.getenv("DATABASE_URL")

def _engine():
    if not DBURL:
        raise RuntimeError("DATABASE_URL not set")
    return create_engine(DBURL)

def average_days_on_market() -> pd.DataFrame:
    """
    DOM per listing = (last_seen - first_seen + 1)
    Then return overall average + distribution.
    """
    eng = _engine()
    q = """
    WITH span AS (
      SELECT f.url,
             f.first_seen,
             l.last_seen,
             (l.last_seen - f.first_seen + 1) AS dom
      FROM listing_first_seen f
      JOIN listing_last_seen  l USING (url)
    )
    SELECT
      AVG(dom)        AS avg_dom_days,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dom) AS median_dom_days,
      COUNT(*)        AS listings_count
    FROM span;
    """
    with eng.begin() as conn:
        return pd.read_sql(q, conn)

def daily_ppsqm_series() -> pd.DataFrame:
    """
    Daily average price_per_sqm across all listings.
    """
    eng = _engine()
    q = """
    SELECT as_of_date, AVG(price_per_sqm) AS avg_ppsqm
    FROM listings
    WHERE price_per_sqm IS NOT NULL AND price_per_sqm > 0
    GROUP BY as_of_date
    ORDER BY as_of_date;
    """
    with eng.begin() as conn:
        return pd.read_sql(q, conn, parse_dates=["as_of_date"])

def ppsqm_growth(window_days: int = 7) -> pd.DataFrame:
    """
    Compute growth of avg ppsqm over a trailing window (e.g., 7, 14, 21 days).
    Returns a time series with pct change from N days prior.
    """
    df = daily_ppsqm_series()
    if df.empty:
        return df.assign(growth_pct=np.nan)

    df = df.sort_values("as_of_date").reset_index(drop=True)
    df["avg_ppsqm_shift"] = df["avg_ppsqm"].shift(window_days)
    df["growth_pct"] = (df["avg_ppsqm"] - df["avg_ppsqm_shift"]) / df["avg_ppsqm_shift"] * 100.0
    return df[["as_of_date", "avg_ppsqm", "growth_pct"]]
