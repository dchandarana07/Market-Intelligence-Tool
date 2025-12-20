"""
Application settings with validation using Pydantic.
Loads configuration from environment variables / .env file.
"""

from pathlib import Path
from typing import Optional, Literal
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # -----------------
    # Google Cloud / Drive / Sheets
    # -----------------
    google_credentials_path: Path = Path("./config/google-credentials.json")
    google_drive_folder_id: str = ""

    # -----------------
    # SerpAPI (Google Jobs)
    # -----------------
    serpapi_key: str = ""

    # -----------------
    # BLS API
    # -----------------
    bls_api_key: str = ""

    # -----------------
    # Lightcast API
    # -----------------
    lightcast_client_id: str = ""
    lightcast_client_secret: str = ""

    # -----------------
    # Email Configuration
    # -----------------
    email_sender: str = ""
    email_app_password: str = ""
    email_smtp_host: str = "smtp.gmail.com"
    email_smtp_port: int = 587

    # -----------------
    # Application Settings
    # -----------------
    environment: Literal["development", "production"] = "development"
    debug: bool = True  # Enable debug logging
    secret_key: str = "dev-secret-key-change-in-production"
    default_sharing_mode: Literal["restricted", "anyone"] = "restricted"

    # Rate limiting
    google_trends_delay_seconds: int = 60
    lightcast_requests_per_second: int = 5

    # -----------------
    # Derived Properties
    # -----------------
    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def google_credentials_available(self) -> bool:
        return self.google_credentials_path.exists() and self.google_drive_folder_id != ""

    @property
    def serpapi_available(self) -> bool:
        return self.serpapi_key != ""

    @property
    def bls_available(self) -> bool:
        # BLS works without key (25 queries/day), but better with key (500/day)
        return True

    @property
    def lightcast_available(self) -> bool:
        return self.lightcast_client_id != "" and self.lightcast_client_secret != ""

    @property
    def email_available(self) -> bool:
        return self.email_sender != "" and self.email_app_password != ""

    def get_available_modules(self) -> dict[str, bool]:
        """Return availability status of each module."""
        return {
            "jobs": self.serpapi_available,  # Jobs requires SerpAPI
            "courses": True,  # Courses always available (Selenium)
            "trends": True,  # Trends always available (pytrends)
            "lightcast": self.lightcast_available,
        }

    def validate_for_run(self) -> list[str]:
        """Validate settings and return list of errors."""
        errors = []

        if not self.google_credentials_available:
            errors.append(
                "Google credentials not configured. "
                "Set GOOGLE_CREDENTIALS_PATH and GOOGLE_DRIVE_FOLDER_ID in .env"
            )

        return errors


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get the settings instance (useful for dependency injection)."""
    return settings
