"""
Google OAuth Authentication Service.

Handles user authentication via Google Sign-In.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional
from google_auth_oauthlib.flow import Flow
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from config.settings import settings

# Allow HTTP for local development (OAuth requires HTTPS by default)
# ONLY enable this for development - production should use HTTPS
if settings.environment == "development":
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

logger = logging.getLogger(__name__)

# OAuth 2.0 scopes for user info
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]


class GoogleAuthService:
    """Service for Google OAuth authentication."""

    def __init__(self):
        self.client_config = self._load_client_config()

    def _load_client_config(self) -> dict:
        """Load OAuth client configuration."""
        # Try to load from file first
        if settings.google_oauth_client_config.exists():
            with open(settings.google_oauth_client_config) as f:
                return json.load(f)

        # Fallback to environment variables
        if settings.google_oauth_client_id and settings.google_oauth_client_secret:
            return {
                "web": {
                    "client_id": settings.google_oauth_client_id,
                    "client_secret": settings.google_oauth_client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["http://localhost:8000/auth/callback"],
                }
            }

        raise ValueError(
            "Google OAuth client configuration not found. "
            "Please set GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET, "
            "or provide config/oauth_client.json"
        )

    def create_flow(self, redirect_uri: str) -> Flow:
        """Create OAuth flow for authentication."""
        flow = Flow.from_client_config(
            self.client_config,
            scopes=SCOPES,
            redirect_uri=redirect_uri,
        )
        return flow

    def get_authorization_url(self, redirect_uri: str, state: str) -> str:
        """Get the authorization URL for user to visit."""
        flow = self.create_flow(redirect_uri)
        authorization_url, _ = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="false",  # Only request explicitly defined scopes
            state=state,
            prompt="select_account",  # Force account selection
        )
        return authorization_url

    def fetch_token(self, redirect_uri: str, authorization_response: str) -> dict:
        """Exchange authorization code for tokens."""
        flow = self.create_flow(redirect_uri)
        flow.fetch_token(authorization_response=authorization_response)

        credentials = flow.credentials

        # Get user info from ID token
        id_info = id_token.verify_oauth2_token(
            credentials.id_token,
            google_requests.Request(),
            credentials.client_id,
        )

        return {
            "email": id_info.get("email"),
            "name": id_info.get("name"),
            "picture": id_info.get("picture"),
            "verified_email": id_info.get("email_verified", False),
        }

    def is_available(self) -> bool:
        """Check if OAuth is properly configured."""
        try:
            self._load_client_config()
            return True
        except (ValueError, FileNotFoundError):
            return False


# Singleton instance
_auth_service: Optional[GoogleAuthService] = None


def get_auth_service() -> GoogleAuthService:
    """Get the singleton auth service instance."""
    global _auth_service
    if _auth_service is None:
        _auth_service = GoogleAuthService()
    return _auth_service
