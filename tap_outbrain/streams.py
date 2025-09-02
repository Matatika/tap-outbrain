"""Stream type classes for tap-outbrain."""

from __future__ import annotations

from datetime import datetime, timezone

from singer_sdk import typing as th  # JSON Schema typing helpers
from typing_extensions import override

from tap_outbrain.client import OutbrainStream
from tap_outbrain.pagination import (
    OutbrainPaginator,
    OutbrainResultsPaginator,
    PeriodicContentPaginator,
)


class MarketerStream(OutbrainStream):
    """Define marketer stream."""

    name = "marketers"
    path = "/marketers"
    records_jsonpath = "$.marketers[*]"
    primary_keys = ("id",)

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
        th.Property(
            "account",
            th.ObjectType(
                th.Property("type", th.StringType),
                th.Property("salesForceAccountId", th.StringType),
            ),
        ),
        th.Property("role", th.StringType),
        th.Property("permissions", th.ArrayType(th.StringType)),
        th.Property("campaignDefaults", th.ObjectType(additional_properties=True)),
    ).to_dict()

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["extraFields"] = "Account"

        return params

    @override
    def get_child_context(self, record, context):
        return {"marketerId": record["id"]}


class CampaignStream(OutbrainStream):
    """Define campaign stream."""

    _check_sorted = True
    _page_size = 50

    parent_stream_type = MarketerStream
    name = "campaigns"
    path = "/marketers/{marketerId}/campaigns"
    records_jsonpath = "$.campaigns[*]"
    primary_keys = ("id",)
    replication_key = "lastModified"
    is_sorted = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("internalId", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("enabled", th.BooleanType),
        th.Property("creationTime", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("cpc", th.NumberType),
        th.Property("autoArchived", th.BooleanType),
        th.Property("minimumCpc", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property(
            "targeting",
            th.ObjectType(
                th.Property("platform", th.ArrayType(th.StringType)),
                th.Property("language", th.StringType),
                th.Property("excludeAdBlockUsers", th.BooleanType),
                th.Property(
                    "nativePlacements",
                    th.ObjectType(
                        th.Property("enabled", th.BooleanType),
                    ),
                ),
                th.Property("includeCellularNetwork", th.BooleanType),
                th.Property("nativePlacementsEnabled", th.BooleanType),
                th.Property("locationsVersion", th.StringType),
            ),
        ),
        th.Property("marketerId", th.StringType),
        th.Property("autoExpirationOfAds", th.IntegerType),
        th.Property("contentType", th.StringType),
        th.Property(
            "budget",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
                th.Property("shared", th.BooleanType),
                th.Property("amount", th.NumberType),
                th.Property("currency", th.StringType),
                th.Property("creationTime", th.DateTimeType),
                th.Property("lastModified", th.DateTimeType),
                th.Property("startDate", th.DateType),
                th.Property("endDate", th.DateType),
                th.Property("runForever", th.BooleanType),
                th.Property("type", th.StringType),
                th.Property("pacing", th.StringType),
                th.Property("dailyTarget", th.NumberType),
            ),
        ),
        th.Property("suffixTrackingCode", th.StringType),
        th.Property(
            "prefixTrackingCode",
            th.ObjectType(
                th.Property("prefix", th.URIType),
                th.Property("encode", th.BooleanType),
            ),
        ),
        th.Property(
            "liveStatus",
            th.ObjectType(
                th.Property("onAirReason", th.StringType),
                th.Property("campaignOnAir", th.BooleanType),
                th.Property("amountSpent", th.NumberType),
                th.Property("onAirModificationTime", th.DateTimeType),
            ),
        ),
        th.Property("readonly", th.BooleanType),
        th.Property("startHour", th.StringType),
        th.Property("onAirType", th.StringType),
        th.Property("objective", th.StringType),
        th.Property("creativeFormat", th.StringType),
        th.Property("dynamicRetargeting", th.BooleanType),
        th.Property("adReviewLevel", th.StringType),
        th.Property(
            "landingPageDetails",
            th.ObjectType(
                th.Property("url", th.URIType),
                th.Property("category", th.IntegerType),
            ),
        ),
        th.Property("isTamCampaign", th.BooleanType),
    ).to_dict()

    @override
    def get_new_paginator(self):
        return OutbrainPaginator(self._page_size)

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["includeArchived"] = True
        params["limit"] = self._page_size
        params["offset"] = next_page_token
        params["sort"] = "+lastModified"

        if starting_last_modified := self.get_starting_timestamp(context):
            delta = datetime.now(tz=timezone.utc) - starting_last_modified

            # API returns data that was last modified up to n+1 days ago
            # (non-inclusively) e.g. a delta of 0 days will return data that last
            # modified up to 1 day/24 hours since
            params["daysToLookBackForChanges"] = delta.days

        return params

    @override
    @property
    def check_sorted(self):
        return self._check_sorted

    @override
    def post_process(self, row, context=None):
        row = super().post_process(row, context)

        starting_last_modified = self.get_starting_timestamp(context)

        # start checking if records are sorted after starting last modified value to
        # account for day-only granularity of daysToLookBackForChanges URL parameter
        if starting_last_modified:
            last_modified = datetime.fromisoformat(row["lastModified"]).replace(
                tzinfo=timezone.utc
            )
            self._check_sorted = last_modified >= starting_last_modified

        return row

    @override
    def get_child_context(self, record, context):
        return context | {"campaignId": record["id"]}


class PromotedLinkStream(OutbrainStream):
    """Define promoted links stream."""

    _page_size = 500

    parent_stream_type = CampaignStream
    name = "promoted_links"
    path = "/campaigns/{campaignId}/promotedLinks"
    records_jsonpath = "$.promotedLinks[*]"
    primary_keys = ("id",)
    ignore_parent_replication_key = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("internalId", th.NumberType),
        th.Property("text", th.StringType),
        th.Property("creationTime", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("url", th.URIType),
        th.Property("siteName", th.StringType),
        th.Property("sectionName", th.StringType),
        th.Property("status", th.StringType),
        th.Property("enabled", th.BooleanType),
        th.Property("cachedImageUrl", th.URIType),
        th.Property("campaignId", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("documentLanguage", th.StringType),
        th.Property(
            "onAirStatus",
            th.ObjectType(
                th.Property("onAir", th.BooleanType),
                th.Property("reason", th.StringType),
            ),
        ),
        th.Property("baseUrl", th.URIType),
        th.Property("documentId", th.StringType),
        th.Property("metaData", th.StringType),
        th.Property(
            "approvalStatus",
            th.ObjectType(
                th.Property("status", th.StringType),
                th.Property("reasons", th.ArrayType(th.StringType)),
                th.Property("isEditable", th.BooleanType),
                th.Property(
                    "primaryElements",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("element", th.StringType),
                            th.Property("reasons", th.ArrayType(th.StringType)),
                        )
                    ),
                ),
            ),
        ),
        th.Property("cpcAdjustment", th.NumberType),
        th.Property("description", th.StringType),
        th.Property(
            "callToAction",
            th.ObjectType(
                th.Property("type", th.StringType),
                th.Property("value", th.StringType),
            ),
        ),
        th.Property("imagePlayMode", th.StringType),
        th.Property("imageType", th.StringType),
        th.Property("language", th.StringType),
        th.Property(
            "imageMetadata",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("requestedImageUrl", th.URIType),
                th.Property("originalImageUrl", th.URIType),
                th.Property("type", th.StringType),
                th.Property("cropType", th.StringType),
            ),
        ),
        th.Property("creativeFormat", th.StringType),
        th.Property("dynamicVisualsEnabled", th.StringType),
    ).to_dict()

    @override
    def get_new_paginator(self):
        return OutbrainPaginator(self._page_size)

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["includeArchived"] = True
        params["limit"] = self._page_size
        params["offset"] = next_page_token
        params["sort"] = "+creationDate"
        params["extraFields"] = ["ImageURL", "ImageMetaData"]

        return params

    @override
    def get_child_context(self, record, context):
        return context | {"promotedLinkId": record["id"]}


class PromotedLinkDailyPerformanceStream(OutbrainStream):
    """Define promoted link daily performance stream."""

    _page_size = 7  # up to a week

    parent_stream_type = CampaignStream
    name = "promoted_link_daily_performance"
    path = "/reports/marketers/{marketerId}/campaigns/{campaignId}/periodicContent"
    records_jsonpath = "$.promotedLinkResults[*]"
    primary_keys = ("promotedLinkId", "date")
    replication_key = "date"
    is_timestamp_replication_key = True
    ignore_parent_replication_key = True

    schema = th.PropertiesList(
        th.Property("promotedLinkId", th.StringType),
        th.Property("date", th.DateType),
        th.Property("impressions", th.NumberType),
        th.Property("clicks", th.NumberType),
        th.Property("totalConversions", th.NumberType),
        th.Property("conversions", th.NumberType),
        th.Property("viewConversions", th.NumberType),
        th.Property("spend", th.NumberType),
        th.Property("ecpc", th.NumberType),
        th.Property("ctr", th.NumberType),
        th.Property("dstFeeCost", th.NumberType),
        th.Property("cpm", th.NumberType),
        th.Property("conversionRate", th.NumberType),
        th.Property("viewConversionRate", th.NumberType),
        th.Property("cpa", th.NumberType),
        th.Property("totalCpa", th.NumberType),
        th.Property("totalSumValue", th.NumberType),
        th.Property("sumValue", th.NumberType),
        th.Property("viewSumValue", th.NumberType),
        th.Property("totalAverageValue", th.NumberType),
        th.Property("averageValue", th.NumberType),
        th.Property("viewAverageValue", th.NumberType),
        th.Property("totalRoas", th.NumberType),
        th.Property("roas", th.NumberType),
        th.Property(
            "conversionMetrics",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("totalConversions", th.NumberType),
                    th.Property("conversions", th.NumberType),
                    th.Property("viewConversions", th.NumberType),
                    th.Property("conversionRate", th.StringType),
                    th.Property("viewConversionRate", th.StringType),
                    th.Property("totalCpa", th.StringType),
                    th.Property("cpa", th.StringType),
                    th.Property("totalSumValue", th.StringType),
                    th.Property("sumValue", th.StringType),
                    th.Property("viewSumValue", th.StringType),
                    th.Property("totalAverageValue", th.StringType),
                    th.Property("averageValue", th.StringType),
                    th.Property("viewAverageValue", th.StringType),
                    th.Property("totalRoas", th.StringType),
                    th.Property("roas", th.StringType),
                )
            ),
        ),
    ).to_dict()

    @override
    def get_new_paginator(self):
        return PeriodicContentPaginator(self._page_size)

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["breakdown"] = "daily"
        params["from"] = self.get_starting_timestamp(context).date()
        params["to"] = datetime.now(tz=timezone.utc).date()
        params["includeArchivedCampaigns"] = True
        params["includeConversionDetails"] = True
        params["limit"] = self._page_size
        params["offset"] = next_page_token
        params["sort"] = "+fromDate"

        return params

    @override
    def parse_response(self, response):
        for record in super().parse_response(response):
            for result in record.pop("results"):
                yield record | result

    @override
    def post_process(self, row, context=None):
        row = super().post_process(row, context)
        row["date"] = row.pop("metadata")["id"]
        row.update(row.pop("metrics"))

        return row

class SectionDailyPerformanceStream(OutbrainStream):
    """Define section daily performance stream."""

    _page_size = 500

    parent_stream_type = CampaignStream
    name = "section_daily_performance"
    path = "/reports/marketers/{marketerId}/sections/date"
    records_jsonpath = "$.results[*]"
    primary_keys = ("campaignId", "date", "id")
    replication_key = "date"
    is_timestamp_replication_key = True
    is_sorted = True
    ignore_parent_replication_key = True

    schema = th.PropertiesList(
        th.Property("campaignId", th.StringType),
        th.Property("date", th.DateType),
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("identifier", th.StringType),
        th.Property("publisherId", th.StringType),
        th.Property("publisherName", th.StringType),
        th.Property("publisherIdentifier", th.StringType),
        th.Property("url", th.URIType),
        th.Property("publisherUrl", th.URIType),
        th.Property(
            "metrics",
            th.ObjectType(
                th.Property("impressions", th.NumberType),
                th.Property("clicks", th.NumberType),
                th.Property("totalConversions", th.NumberType),
                th.Property("conversions", th.NumberType),
                th.Property("viewConversions", th.NumberType),
                th.Property("spend", th.NumberType),
                th.Property("ecpc", th.NumberType),
                th.Property("ctr", th.NumberType),
                th.Property("dstFeeCost", th.NumberType),
                th.Property("conversionRate", th.NumberType),
                th.Property("viewConversionRate", th.NumberType),
                th.Property("cpa", th.NumberType),
                th.Property("totalCpa", th.NumberType),
                th.Property("totalSumValue", th.NumberType),
                th.Property("sumValue", th.NumberType),
                th.Property("viewSumValue", th.NumberType),
                th.Property("totalAverageValue", th.NumberType),
                th.Property("averageValue", th.NumberType),
                th.Property("viewAverageValue", th.NumberType),
                th.Property("totalRoas", th.NumberType),
                th.Property("roas", th.NumberType),
            ),
        ),
    ).to_dict()

    @override
    def get_new_paginator(self):
        return OutbrainResultsPaginator(self._page_size)

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["campaignId"] = context["campaignId"]
        params["from"] = self.get_starting_timestamp(context).date()
        params["to"] = datetime.now(tz=timezone.utc).date()
        params["includeArchivedCampaigns"] = True
        params["includeConversionDetails"] = True
        params["limit"] = self._page_size
        params["offset"] = next_page_token

        return params

    @override
    def parse_response(self, response):
        for record in super().parse_response(response):
            for section in record.pop("sections"):
                yield record | section

    @override
    def post_process(self, row, context=None):
        row = super().post_process(row, context)
        row.update(row.pop("metadata"))
        row.update(row.pop("metrics"))

        del row["totalResults"]

        return row


class BudgetStream(OutbrainStream):
    """Define budget stream."""

    parent_stream_type = MarketerStream
    name = "budgets"
    path = "/marketers/{marketerId}/budgets"
    records_jsonpath = "$.budgets[*]"
    primary_keys = ("id",)

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("shared", th.BooleanType),
        th.Property("amount", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("amountRemaining", th.NumberType),
        th.Property("amountSpent", th.NumberType),
        th.Property("creationTime", th.DateTimeType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("startDate", th.DateType),
        th.Property("endDate", th.DateType),
        th.Property("runForever", th.BooleanType),
        th.Property("type", th.StringType),
        th.Property("pacing", th.StringType),
        th.Property("dailyTarget", th.NumberType),
    ).to_dict()
