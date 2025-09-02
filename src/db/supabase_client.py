from __future__ import annotations
import os, time, json
from typing import Any, Dict, Iterable, Optional, Tuple, List
import urllib.request
import urllib.error

DEFAULT_RETRY_STATUSES = {429, 500, 502, 503, 504}

class SupabaseClient:
    def __init__(
        self,
        url: Optional[str] = None,
        service_role_key: Optional[str] = None,
        retry_statuses: Optional[set] = None,
    ):
        self.url = (url or os.getenv("SUPABASE_URL", "")).rstrip("/")
        self.key = service_role_key or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
        if not self.url or not self.key:
            raise RuntimeError("SupabaseClient: SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY missing.")
        self.retry_statuses = retry_statuses or DEFAULT_RETRY_STATUSES

    # low-level request with simple exponential backoff
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, str]] = None,
        json_body: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 4,
        backoff_base: float = 0.8,
    ) -> Tuple[int, bytes]:
        assert path.startswith("/"), "path must start with /"
        q = ""
        if params:
            from urllib.parse import urlencode
            q = "?" + urlencode(params)
        url = f"{self.url}{path}{q}"

        payload: Optional[bytes] = None
        req_headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        }
        if headers:
            req_headers.update(headers)
        if json_body is not None:
            payload = json.dumps(json_body).encode("utf-8")

        attempt = 0
        while True:
            attempt += 1
            req = urllib.request.Request(url, data=payload, method=method, headers=req_headers)
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    return resp.getcode(), resp.read()
            except urllib.error.HTTPError as e:
                status = e.code
                body = e.read()
                if status in self.retry_statuses and attempt <= max_retries:
                    time.sleep(backoff_base * (2 ** (attempt - 1)))
                    continue
                return status, body
            except urllib.error.URLError as e:
                # network error; retry
                if attempt <= max_retries:
                    time.sleep(backoff_base * (2 ** (attempt - 1)))
                    continue
                raise RuntimeError(f"Network error calling {url}: {e}") from e

    # PostgREST upsert (merge duplicates) with optional on_conflict
    def upsert_rows(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        *,
        on_conflict: Optional[str] = None,
        return_representation: bool = False,
    ) -> Tuple[int, bytes]:
        params = {}
        if on_conflict:
            params["on_conflict"] = on_conflict
        headers = {
            # upsert behavior
            "Prefer": f"resolution=merge-duplicates,return={'representation' if return_representation else 'minimal'}"
        }
        return self._request(
            "POST",
            f"/rest/v1/{table}",
            params=params,
            json_body=rows,
            headers=headers,
        )
