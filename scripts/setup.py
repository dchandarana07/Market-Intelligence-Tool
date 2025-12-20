#!/usr/bin/env python3
"""
Setup script for Market Intelligence Tool.

This script helps verify your configuration and test connections.

Usage:
    python scripts/setup.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings


def check_mark(condition: bool) -> str:
    return "[OK]" if condition else "[MISSING]"


def main():
    print("=" * 60)
    print("  Market Intelligence Tool - Configuration Check")
    print("=" * 60)
    print()

    # Check .env file
    env_file = Path(__file__).parent.parent / ".env"
    if not env_file.exists():
        print("WARNING: .env file not found!")
        print("  Copy .env.example to .env and fill in your values.")
        print()

    # Check each configuration
    print("Configuration Status:")
    print("-" * 40)

    # Google Sheets
    google_ok = settings.google_credentials_available
    print(f"  {check_mark(google_ok)} Google Sheets/Drive")
    if not google_ok:
        if not settings.google_credentials_path.exists():
            print(f"      - Credentials file not found: {settings.google_credentials_path}")
        if not settings.google_drive_folder_id:
            print("      - GOOGLE_DRIVE_FOLDER_ID not set")

    # SerpAPI
    serpapi_ok = settings.serpapi_available
    print(f"  {check_mark(serpapi_ok)} SerpAPI (Jobs module)")
    if not serpapi_ok:
        print("      - SERPAPI_KEY not set")
        print("      - Get free key: https://serpapi.com/users/sign_up")

    # BLS API
    bls_ok = bool(settings.bls_api_key)
    print(f"  {check_mark(bls_ok)} BLS API (optional, increases rate limit)")
    if not bls_ok:
        print("      - BLS_API_KEY not set (optional)")
        print("      - Register: https://data.bls.gov/registrationEngine/")

    # Lightcast
    lightcast_ok = settings.lightcast_available
    print(f"  {check_mark(lightcast_ok)} Lightcast API (Skills module)")
    if not lightcast_ok:
        print("      - LIGHTCAST_CLIENT_ID or LIGHTCAST_CLIENT_SECRET not set")
        print("      - Apply: https://lightcast.io/open-skills/access")

    # Email
    email_ok = settings.email_available
    print(f"  {check_mark(email_ok)} Email notifications")
    if not email_ok:
        print("      - EMAIL_SENDER or EMAIL_APP_PASSWORD not set")
        print("      - Setup Gmail App Password:")
        print("        https://myaccount.google.com/apppasswords")

    print()
    print("-" * 40)

    # Summary
    all_required = google_ok
    all_modules = serpapi_ok and lightcast_ok

    if all_required:
        print("READY: Core requirements met. You can run the tool.")
        if not all_modules:
            print("       Some modules may be unavailable.")
    else:
        print("NOT READY: Please configure Google Sheets/Drive first.")

    print()
    print("To start the server, run:")
    print("  python run.py")
    print()


if __name__ == "__main__":
    main()
