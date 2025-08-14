"""Outbrain tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_outbrain import streams


class TapOutbrain(Tap):
    """Outbrain tap class."""

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
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.OutbrainStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupsStream(self),
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    TapOutbrain.cli()
