"""Stream type classes for tap-outbrain."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_outbrain.client import OutbrainStream


class MarketerStream(OutbrainStream):
    """Define marketer stream."""

    name = "marketers"
    path = "/marketers"
    records_jsonpath = "$.marketers[*]"
    primary_keys = ("id",)
    replication_key = "lastModified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("enabled", th.BooleanType),
        th.Property("currency", th.StringType),
        th.Property("creationTime", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property(
            "blockedSites",
            th.ObjectType(
                th.Property(
                    "blockedPublishers",
                    th.ObjectType(
                        th.Property("id", th.StringType),
                        th.Property("name", th.StringType),
                        th.Property(
                            "publisher",
                            th.ObjectType(
                                th.Property("id", th.StringType),
                                th.Property("name", th.StringType),
                            ),
                        ),
                        th.Property("creationTime", th.DateTimeType),
                        th.Property("modifiedBy", th.StringType),
                    ),
                ),
                th.Property(
                    "blockedSections",
                    th.ObjectType(
                        th.Property("id", th.StringType),
                        th.Property("name", th.StringType),
                        th.Property(
                            "publisher",
                            th.ObjectType(
                                th.Property("id", th.StringType),
                                th.Property("name", th.StringType),
                            ),
                        ),
                        th.Property("creationTime", th.DateTimeType),
                        th.Property("modifiedBy", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property("useFirstPartyCookie", th.BooleanType),
        th.Property("role", th.StringType),
        th.Property("permissions", th.ArrayType(th.StringType)),
        th.Property("campaignDefaults", th.ObjectType(additional_properties=True)),
    ).to_dict()
