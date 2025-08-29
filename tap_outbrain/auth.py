"""Outbrain Authentication."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING

import requests
from requests.auth import HTTPBasicAuth
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing_extensions import override

if TYPE_CHECKING:
    from tap_outbrain.client import OutbrainStream

import platformdirs

ACCESS_TOKEN_EXPIRE_AFTER_DAYS = 30


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class OutbrainAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Outbrain."""

    @override
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # static access token expiry of 30 days
        # https://amplifyv01.docs.apiary.io/#reference/authentications
        self.expires_in = timedelta(days=ACCESS_TOKEN_EXPIRE_AFTER_DAYS).total_seconds()

        self.access_token_file = (
            Path(platformdirs.user_cache_dir(self.tap_name, ensure_exists=True))
            / "access_token"
        )

        if not self.access_token_file.exists():
            return

        self.logger.info("Using access token from cache: %s", self.access_token_file)
        self.access_token = self.access_token_file.read_text()

        self.last_refreshed = datetime.fromtimestamp(
            self.access_token_file.stat().st_mtime,
            tz=timezone.utc,
        )

        if not self.is_token_valid():
            self.logger.info(
                "Cached access token modified more than %d days ago - assuming expired",
                ACCESS_TOKEN_EXPIRE_AFTER_DAYS,
            )

    @override
    @property
    def oauth_request_body(self):
        return {}

    @override
    def update_access_token(self):
        token_response = requests.get(
            self.auth_endpoint,
            auth=HTTPBasicAuth(self.config["username"], self.config["password"]),
            timeout=60,
        )

        try:
            token_response.raise_for_status()
        except requests.HTTPError as ex:
            msg = f"Failed OAuth login, response was '{token_response.json()}'"

            if token_response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
                secs = int(token_response.headers["rate-limit-msec-left"]) / 1000
                mins, r_secs = divmod(secs, 60)
                hrs, r_mins = divmod(mins, 60)

                msg += f" (remaining: {hrs:.0f}h {r_mins:.0f}m {r_secs:.0f}s)"

            msg = f"{msg}. {ex}"
            raise RuntimeError(msg) from ex

        self.logger.info("OAuth authorization attempt was successful.")

        token_json = token_response.json()
        self.access_token = token_json["OB-TOKEN-V1"]
        self.access_token_file.write_text(self.access_token)

        self.last_refreshed = datetime.fromtimestamp(
            self.access_token_file.stat().st_mtime,
            tz=timezone.utc,
        )

    @override
    def authenticate_request(self, request):
        request = super().authenticate_request(request)

        del request.headers["Authorization"]
        request.headers["OB-TOKEN-V1"] = self.access_token

        return request

    @classmethod
    def create_for_stream(cls, stream: OutbrainStream) -> OutbrainAuthenticator:
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            auth_endpoint=f"{stream.url_base}/login",
        )
