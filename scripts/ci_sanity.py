import os
from pathlib import Path
from src import config

print("ROOT:", config.ROOT)

# 1) ensure portals config loads
cfg = config.load_portals_config()
print("portals.json keys:", list(cfg.keys()))

# 2) dev vs prod guard
os.environ["ENV"] = "prod"
try:
    config.validate_prod()  # should raise if missing supabase envs
except RuntimeError as e:
    print("validate_prod() correctly blocked missing secrets:", e)
else:
    raise SystemExit("ERROR: validate_prod() should have failed without secrets")

# 3) switch back to dev and re-import
os.environ["ENV"] = "dev"
from importlib import reload
reload(config)
print("Sanity OK (dev).")
