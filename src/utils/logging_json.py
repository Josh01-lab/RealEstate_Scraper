import json, logging
from datetime import datetime

def log_json(level: str, event: str, **kv):
    rec = {"ts": datetime.utcnow().isoformat()+"Z", "level": level, "event": event}
    rec.update(kv)
    logging.getLogger().info(json.dumps(rec))
