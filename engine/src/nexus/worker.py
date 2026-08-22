"""Worker process entry point for future database-backed jobs."""

import asyncio

from nexus.settings import EngineSettings


async def run_worker(settings: EngineSettings | None = None) -> None:
    """Validate startup settings without performing provider or broker work."""
    _settings = settings or EngineSettings()
    await asyncio.sleep(0)


def main() -> None:
    """Run the worker process entry point."""
    asyncio.run(run_worker())
