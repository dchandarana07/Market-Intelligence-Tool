"""
Google Sheets service using OAuth2 user authentication.

This approach uses the user's own Google Drive quota instead of a service account.
The user needs to authorize once through a browser.
"""

import logging
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Optional, Literal
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config.settings import settings

logger = logging.getLogger(__name__)

# Scopes required
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Token storage path
TOKEN_PATH = Path("./config/oauth_token.pickle")
CREDENTIALS_PATH = Path("./config/oauth_credentials.json")


class GoogleSheetsOAuthService:
    """
    Google Sheets service using OAuth2 user authentication.

    Files are created in the user's own Drive, using their quota.
    """

    def __init__(self):
        self._credentials: Optional[Credentials] = None
        self._sheets_service = None
        self._drive_service = None

    def _get_credentials(self) -> Credentials:
        """Get or refresh OAuth2 credentials."""
        if self._credentials and self._credentials.valid:
            return self._credentials

        # Try to load existing token
        if TOKEN_PATH.exists():
            with open(TOKEN_PATH, "rb") as token:
                self._credentials = pickle.load(token)

        # Check if credentials are valid or need refresh
        if self._credentials and self._credentials.expired and self._credentials.refresh_token:
            try:
                self._credentials.refresh(Request())
            except Exception:
                self._credentials = None

        # If no valid credentials, need to authenticate
        if not self._credentials or not self._credentials.valid:
            if not CREDENTIALS_PATH.exists():
                raise FileNotFoundError(
                    f"OAuth credentials file not found at: {CREDENTIALS_PATH}\n"
                    "Please download OAuth client credentials from Google Cloud Console."
                )

            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_PATH), SCOPES
            )
            self._credentials = flow.run_local_server(port=8080)

            # Save the token for future use
            with open(TOKEN_PATH, "wb") as token:
                pickle.dump(self._credentials, token)
            logger.info("OAuth token saved")

        return self._credentials

    def _get_services(self):
        """Get Sheets and Drive API services."""
        creds = self._get_credentials()

        if self._sheets_service is None:
            self._sheets_service = build("sheets", "v4", credentials=creds)
        if self._drive_service is None:
            self._drive_service = build("drive", "v3", credentials=creds)

        return self._sheets_service, self._drive_service

    def is_available(self) -> bool:
        """Check if the service is available (OAuth credentials exist or user is authenticated)."""
        return CREDENTIALS_PATH.exists() or TOKEN_PATH.exists()

    def is_authenticated(self) -> bool:
        """Check if user is already authenticated."""
        if TOKEN_PATH.exists():
            try:
                with open(TOKEN_PATH, "rb") as token:
                    creds = pickle.load(token)
                    return creds and creds.valid
            except Exception:
                return False
        return False

    def authenticate(self) -> bool:
        """Trigger authentication flow. Returns True if successful."""
        try:
            self._get_credentials()
            return True
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False

    def create_output(
        self,
        title: str,
        data: dict[str, pd.DataFrame],
        folder_id: Optional[str] = None,
        share_with: Optional[str] = None,
        sharing_mode: Literal["restricted", "anyone"] = "restricted",
        notify: bool = False,
    ) -> dict:
        """
        Create a new Google Spreadsheet with the provided data.

        Args:
            title: Title for the spreadsheet
            data: Dictionary mapping sheet names to DataFrames
            folder_id: Optional folder ID to move the file to
            share_with: Email address to share with
            sharing_mode: "restricted" or "anyone"
            notify: Whether to send email notification when sharing

        Returns:
            Dictionary with spreadsheet_id, spreadsheet_url, etc.
        """
        sheets_service, drive_service = self._get_services()

        # Generate timestamped title
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        full_title = f"{title} - {timestamp}"

        logger.info(f"Creating spreadsheet: {full_title}")

        # Create empty spreadsheet
        spreadsheet_body = {"properties": {"title": full_title}}
        spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet_body).execute()
        spreadsheet_id = spreadsheet["spreadsheetId"]
        spreadsheet_url = spreadsheet["spreadsheetUrl"]

        logger.info(f"Spreadsheet created with ID: {spreadsheet_id}")

        # Write data to sheets
        self._write_data(sheets_service, spreadsheet_id, data)

        # Move to folder if specified
        folder_url = None
        target_folder = folder_id or settings.google_drive_folder_id
        if target_folder:
            try:
                file = drive_service.files().get(
                    fileId=spreadsheet_id, fields="parents"
                ).execute()
                previous_parents = ",".join(file.get("parents", []))

                drive_service.files().update(
                    fileId=spreadsheet_id,
                    addParents=target_folder,
                    removeParents=previous_parents,
                    fields="id, parents",
                ).execute()
                folder_url = f"https://drive.google.com/drive/folders/{target_folder}"
                logger.info(f"Moved to folder: {target_folder}")
            except Exception as e:
                logger.warning(f"Could not move to folder: {e}")

        # Set sharing permissions
        shared_with_list = []
        try:
            if sharing_mode == "anyone":
                drive_service.permissions().create(
                    fileId=spreadsheet_id,
                    body={"type": "anyone", "role": "reader"},
                ).execute()
                shared_with_list.append("Anyone with link")
            elif share_with:
                drive_service.permissions().create(
                    fileId=spreadsheet_id,
                    body={"type": "user", "role": "reader", "emailAddress": share_with},
                    sendNotificationEmail=notify,
                ).execute()
                shared_with_list.append(share_with)
                logger.info(f"[GoogleSheets] Shared with {share_with}, notify={notify}")
        except Exception as e:
            logger.warning(f"Could not set sharing: {e}")

        return {
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_url": spreadsheet_url,
            "folder_url": folder_url,
            "shared_with": shared_with_list,
            "title": full_title,
        }

    def _write_data(
        self,
        sheets_service,
        spreadsheet_id: str,
        data: dict[str, pd.DataFrame],
    ) -> None:
        """Write DataFrames to sheets."""
        # Get existing sheets
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        existing_sheets = spreadsheet.get("sheets", [])
        first_sheet_id = existing_sheets[0]["properties"]["sheetId"] if existing_sheets else None

        requests = []
        batch_data = []
        sheet_index = 0

        for sheet_name, df in data.items():
            if df.empty:
                continue

            # Truncate sheet name to 100 chars (Google Sheets limit)
            safe_name = sheet_name[:100]

            if sheet_index == 0 and first_sheet_id is not None:
                # Rename first sheet
                requests.append({
                    "updateSheetProperties": {
                        "properties": {"sheetId": first_sheet_id, "title": safe_name},
                        "fields": "title",
                    }
                })
                target_sheet = safe_name
            else:
                # Add new sheet
                requests.append({
                    "addSheet": {"properties": {"title": safe_name}}
                })
                target_sheet = safe_name

            # Prepare data for batch update
            df_clean = df.fillna("")
            for col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str)

            values = [df.columns.tolist()] + df_clean.values.tolist()
            batch_data.append({
                "range": f"'{safe_name}'!A1",
                "values": values,
            })

            sheet_index += 1

        # Execute sheet structure changes
        if requests:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            ).execute()

        # Write all data
        if batch_data:
            sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "RAW",
                    "data": batch_data,
                },
            ).execute()

        logger.info(f"Wrote data to {len(batch_data)} sheets")


# Singleton instance
_oauth_service: Optional[GoogleSheetsOAuthService] = None


def get_oauth_sheets_service() -> GoogleSheetsOAuthService:
    """Get the singleton OAuth Sheets service instance."""
    global _oauth_service
    if _oauth_service is None:
        _oauth_service = GoogleSheetsOAuthService()
    return _oauth_service
