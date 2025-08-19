"""REST client handling, including OutbrainStream base class."""

from __future__ import annotations

import math
import typing as t
from functools import cached_property

from singer_sdk.streams import RESTStream
from typing_extensions import override

from tap_outbrain.auth import OutbrainAuthenticator

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Auth


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
