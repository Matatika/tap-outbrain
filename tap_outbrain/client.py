"""REST client handling, including OutbrainStream base class."""

from __future__ import annotations

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
        # Default exponential backoff, used for any retriable error that isn't a
        # rate limit (dropped connections, timeouts, 5xx, ...). Primed with a
        # `send(None)` to mirror how `backoff` initialises a wait generator.
        default_wait = super().backoff_wait_generator()
        default_wait.send(None)

        exception = yield

        while True:
            if (
                isinstance(exception, RetriableAPIError)
                and exception.response.status_code == HTTPStatus.TOO_MANY_REQUESTS
            ):
                remaining_ms = exception.response.headers["rate-limit-msec-left"]
                wait = math.ceil(float(remaining_ms) / 1000)
            else:
                wait = default_wait.send(exception)

            exception = yield wait

    @override
    def backoff_max_tries(self):
        return 8

    @cached_property
    def include_archived(self):
        """Whether or not to include archived data."""
        return self.name in self.config["include_archived"]
