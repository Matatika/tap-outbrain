"""Pagination classes for tap-outbrain."""

from __future__ import annotations

from singer_sdk.pagination import BaseOffsetPaginator
from typing_extensions import override


class OutbrainPaginator(BaseOffsetPaginator):
    """Outbrain paginator."""

    @override
    def has_more(self, response):
        next_offset = self.current_value + self._page_size
        total_count: int = response.json()["totalCount"]

        return next_offset < total_count
