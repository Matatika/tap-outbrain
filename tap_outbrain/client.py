"""REST client handling, including OutbrainStream base class."""

from __future__ import annotations

import math
import typing as t
from functools import cached_property

from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream
from typing_extensions import override

from tap_outbrain.auth import OutbrainAuthenticator

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Auth, Context


class OutbrainStream(RESTStream):
    """Outbrain stream class."""

    url_base = "https://api.outbrain.com/amplify/v0.1"

    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return OutbrainAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        return {}

    def get_new_paginator(self) -> BaseAPIPaginator | None:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance, or ``None`` to indicate pagination
            is not supported.
        """
        return super().get_new_paginator()

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    @override
    def backoff_wait_generator(self):
        def _backoff_from_headers(retriable_api_error: requests.HTTPError):
            response_headers = retriable_api_error.response.headers
            remaining_ms = response_headers["rate-limit-msec-left"]
            return math.ceil(float(remaining_ms) / 1000)

        return self.backoff_runtime(value=_backoff_from_headers)

    @override
    def backoff_jitter(self, value):
        return value  # no jitter
