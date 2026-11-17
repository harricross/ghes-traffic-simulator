"""Small, dependency-free functions with real unit tests."""

import hashlib
import json
import re

_SEPARATORS = re.compile(r"[^a-z0-9]+")


def slugify(text):
    slug = _SEPARATORS.sub("-", text.lower()).strip("-")
    return slug or "item"


def chunk(items, size):
    if size <= 0:
        raise ValueError("size must be positive")
    return [items[i:i + size] for i in range(0, len(items), size)]


def shard(items, index, total):
    if total <= 0 or not 0 <= index < total:
        raise ValueError("index must be in [0, total)")
    return [item for position, item in enumerate(items) if position % total == index]


def manifest(entries):
    return json.dumps(dict(sorted(entries.items())), separators=(",", ":"))


def digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
