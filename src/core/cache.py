"""LRU cache for core operations."""

import time
from collections import OrderedDict
from typing import Any, Optional
import asyncio
import functools


class TTLCache:
    """Time-to-live cache with async support."""

    def __init__(self, maxsize: int = 128, ttl: float = 300.0):
        self._cache: OrderedDict = OrderedDict()
        self._maxsize = maxsize
        self._ttl = ttl
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if time.monotonic() - entry["time"] < self._ttl:
                    self._cache.move_to_end(key)
                    self._hits += 1
                    return entry["value"]
                del self._cache[key]
            self._misses += 1
            return None

    async def set(self, key: str, value: Any) -> None:
        async with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = {"value": value, "time": time.monotonic()}
            if len(self._cache) > self._maxsize:
                self._cache.popitem(last=False)

    @property
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0

    def clear(self) -> None:
        self._cache.clear()
        self._hits = 0
        self._misses = 0
