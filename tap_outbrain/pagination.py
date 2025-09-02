"""Pagination classes for tap-outbrain."""

from __future__ import annotations

from singer_sdk.pagination import BaseOffsetPaginator
from typing_extensions import override


class OutbrainPaginator(BaseOffsetPaginator):
    """Outbrain paginator."""

    @override
    def __init__(self, page_size, total_key="totalCount") -> None:
        super().__init__(0, page_size)
        self._total_key = total_key

    @override
    def has_more(self, response):
        next_offset = self.get_next(response)
        total: int = response.json()[self._total_key]

        return next_offset < total


class OutbrainResultsPaginator(BaseOffsetPaginator):
    """Outbrain results paginator."""

    @override
    def __init__(self, page_size) -> None:
        super().__init__(0, page_size)

    @override
    def has_more(self, response):
        next_offset = self.get_next(response)
        results: list[dict] = response.json()["results"]
        total: int = max(r["totalResults"] for r in results) if results else 0

        return next_offset < total


class PeriodicContentPaginator(BaseOffsetPaginator):
    """Periodic content paginator."""

    @override
    def __init__(self, page_size) -> None:
        super().__init__(0, page_size)

    @override
    def has_more(self, response):
        next_offset = self.get_next(response)
        total: int = response.json()["promotedLinkResults"][0]["totalResults"]

        return next_offset < total
