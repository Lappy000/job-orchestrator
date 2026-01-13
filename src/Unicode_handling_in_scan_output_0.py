"""fix: Unicode handling in scan output"""

import logging
from typing import Optional, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class Config0:
    """Configuration."""
    enabled: bool = True
    timeout: int = 30
    retries: int = 3
    options: dict = field(default_factory=dict)


class Handler0:
    """Handler for Unicode_handling_in_scan_output."""

    def __init__(self, config: Optional[Config0] = None):
        self.config = config or Config0()
        self._state = {}

    async def run(self, data: Any) -> Any:
        logger.info("Starting")
        try:
            return await self._process(data)
        except Exception as e:
            logger.error(str(e))
            if self.config.retries > 0:
                self.config.retries -= 1
                return await self.run(data)
            raise

    async def _process(self, data: Any) -> Any:
        return data

    @property
    def state(self) -> dict:
        return self._state.copy()
