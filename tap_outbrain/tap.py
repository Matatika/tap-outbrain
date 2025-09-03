"""Outbrain tap class."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from typing_extensions import override

from tap_outbrain import streams

STREAM_TYPES = [
    streams.MarketerStream,
    streams.CampaignStream,
    streams.BudgetStream,
    streams.PromotedLinkStream,
    streams.PromotedLinkDailyPerformanceStream,
    streams.SectionDailyPerformanceStream,
]


class TapOutbrain(Tap):
    """Outbrain tap class."""

    _start_date = datetime.now(tz=timezone.utc) - timedelta(days=365)

    name = "tap-outbrain"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            title="Username",
            description="The username to use for authentication",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            secret=True,
            title="Password",
            description="The password to use for authentication",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            title="Start Date",
            description="Initial date to start extracting data from",
            default=_start_date.isoformat(),
        ),
        th.Property(
            "include_archived",
            th.ArrayType(th.StringType),
            title="Include archived",
            description="Whether or not to extract archived data per stream",
            default=[],
        ),
    ).to_dict()

    @override
    def discover_streams(self):
        return [stream_cls(tap=self) for stream_cls in STREAM_TYPES]


if __name__ == "__main__":
    TapOutbrain.cli()
