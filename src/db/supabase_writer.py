from typing import List, Dict
import time
from .supabase_client import get_client  # your minimal client

class SupabaseWriter:
    def __init__(self, batch_size: int = 100, retries: int = 3):
        self.client = get_client()
        self.batch_size = batch_size
        self.retries = retries
        self.buffer: List[Dict] = []

    def add(self, row: Dict):
        self.buffer.append(row)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        for attempt in range(1, self.retries + 1):
            try:
                self.client.table("listings").upsert(self.buffer).execute()
                self.buffer.clear()
                return
            except Exception as e:
                if attempt == self.retries:
                    raise
                time.sleep(2 ** attempt)  # backoff

    def close(self):
        self.flush()
