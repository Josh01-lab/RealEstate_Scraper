import os
from typing import Optional
from supabase import create_client, Client
from dotenv import load_dotenv, find_dotenv

# Always try to load .env (use current working directory)
_ENV_PATH = find_dotenv(usecwd=True)
if _ENV_PATH:
    print(f"[supabase_client] .env -> {_ENV_PATH}")
    # override=True so shell/parent vars don't block your .env values
    load_dotenv(_ENV_PATH, override=True)

def _get_env(name: str) -> Optional[str]:
    # Read at call-time (not at import-time) to avoid stale values.
    val = os.getenv(name)
    return val.strip() if isinstance(val, str) else val

def debug_env() -> None:
    """Quick visibility into what this module sees."""
    url = _get_env("SUPABASE_URL")
    key = _get_env("SUPABASE_SERVICE_ROLE_KEY")
    print(f"[debug_env] SUPABASE_URL present: {bool(url)} value: {url!r}")
    # mask the key: show only length
    print(f"[debug_env] SUPABASE_SERVICE_ROLE_KEY present: {bool(key)} length: {len(key) if key else 0}")

def get_client() -> Client:
    """Create a Supabase client using env vars. Raises with a clear message if missing."""
    url = _get_env("SUPABASE_URL")
    key = _get_env("SUPABASE_SERVICE_ROLE_KEY")

    missing = [n for n, v in {
        "SUPABASE_URL": url,
        "SUPABASE_SERVICE_ROLE_KEY": key,
    }.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing {missing}. Ensure they are set in .env (repo root) or in the environment."
        )

    return create_client(url, key)