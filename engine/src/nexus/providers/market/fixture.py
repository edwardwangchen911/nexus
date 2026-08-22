"""Strictly offline fixture market provider."""

from collections.abc import Mapping

from nexus.providers.base import BarsRequest, PriceBar, ProviderPage


class FixtureMarketDataProvider:
    """Return only pages explicitly injected by tests or fixture composition."""

    def __init__(self, pages: Mapping[str | None, ProviderPage[PriceBar]]) -> None:
        self._pages = dict(pages)

    async def fetch_bars(
        self, request: BarsRequest, cursor: str | None = None
    ) -> ProviderPage[PriceBar]:
        del request
        try:
            return self._pages[cursor]
        except KeyError as error:
            raise KeyError(f"fixture cursor not found: {cursor!r}") from error
