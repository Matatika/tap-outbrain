"""Outbrain Authentication."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

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

        delta = self.last_refreshed + timedelta(days=ACCESS_TOKEN_EXPIRE_AFTER_DAYS)
        self.expires_in = (delta - datetime.now(tz=timezone.utc)).total_seconds()

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
        super().update_access_token()
        self.access_token_file.write_text(self.access_token)

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
        username_password = ":".join(
            [
                stream.config["username"],
                stream.config["password"],
            ]
        )

        return cls(
            stream=stream,
            auth_endpoint=f"{stream.url_base}/login",
            oauth_headers={
                "Authorization": base64.b64encode(username_password.encode()),
            },
        )
