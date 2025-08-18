"""Pagination classes for tap-outbrain."""

from __future__ import annotations

from singer_sdk.pagination import BaseOffsetPaginator
from typing_extensions import override


class OutbrainPaginator(BaseOffsetPaginator):
    """Outbrain paginator."""

    @override
    def __init__(self, start_value, page_size, total_key="totalCount") -> None:
        super().__init__(start_value, page_size)
        self._total_key = total_key

    @override
    def has_more(self, response):
        next_offset = self.current_value + self._page_size
        total: int = response.json()[self._total_key]

        return next_offset < total
