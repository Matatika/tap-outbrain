"""REST client handling, including OutbrainStream base class."""

from __future__ import annotations

import contextlib
import math
from functools import cached_property
from http import HTTPStatus

from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.streams import RESTStream
from typing_extensions import override

from tap_outbrain.auth import OutbrainAuthenticator


class OutbrainStream(RESTStream):
    """Outbrain stream class."""

    url_base = "https://api.outbrain.com/amplify/v0.1"

    @override
    @cached_property
    def authenticator(self):
        return OutbrainAuthenticator.create_for_stream(self)

    @override
    def backoff_wait_generator(self):
        def _backoff_from_headers(retriable_api_error: RetriableAPIError):
            if retriable_api_error.response.status_code != HTTPStatus.TOO_MANY_REQUESTS:
                raise retriable_api_error

            response_headers = retriable_api_error.response.headers
            remaining_ms = response_headers["rate-limit-msec-left"]
            return math.ceil(float(remaining_ms) / 1000)

        return self.backoff_runtime(value=_backoff_from_headers)

    @override
    def backoff_max_tries(self):
        return 8

    @override
    def backoff_runtime(self, *, value):
        exception = yield

        with contextlib.suppress(RetriableAPIError):
            while True:
                exception = yield value(exception)

        # fallback to default backoff implementation
        wait_gen = super().backoff_wait_generator()

        next(wait_gen)  # skip first, always None

        yield from wait_gen

    @cached_property
    def include_archived(self):
        """Whether or not to include archived data."""
        return self.name in self.config["include_archived"]
