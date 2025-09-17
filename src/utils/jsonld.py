from __future__ import annotations
from typing import Any, Dict, Iterable, Generator, Optional, List

def _jsonld_iter(obj: Any) -> Generator[Dict[str, Any], None, None]:
    """Recursively yield every dict node in a JSON/JSON-LD tree."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _jsonld_iter(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _jsonld_iter(it)

def extract_jsonld_blocks(scripts: Iterable[str]) -> List[Dict[str, Any]]:
    """Given the text of <script type='application/ld+json'> tags, parse and return all dict blocks."""
    import json
    blocks: List[Dict[str, Any]] = []
    for txt in scripts:
        try:
            data = json.loads(txt)
            # Flatten: we want only dicts, lists will be walked by _jsonld_iter anyway
            for node in _jsonld_iter(data):
                blocks.append(node)
        except Exception:
            continue
    return blocks

def find_first(node_list: List[Dict[str, Any]], *types: str) -> Optional[Dict[str, Any]]:
    """Return the first JSON-LD node whose @type matches any of *types."""
    tset = set(types)
    for n in node_list:
        t = n.get("@type")
        if isinstance(t, list):
            if tset & set(map(str, t)):
                return n
        elif isinstance(t, str) and t in tset:
            return n
    return None
