"""
Google Sheets and Drive service for creating and managing output spreadsheets.

Handles:
- Creating new spreadsheets
- Writing data to multiple tabs
- Moving files to specific Drive folders
- Setting sharing permissions
- Generating shareable links
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Literal
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings

logger = logging.getLogger(__name__)


# Google API scopes needed
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class GoogleSheetsService:
    """
    Service for creating and managing Google Sheets outputs.

    Usage:
        service = GoogleSheetsService()
        spreadsheet_url = service.create_output(
            title="Market Intelligence - Cybersecurity",
            data={
                "Jobs": jobs_dataframe,
                "BLS Data": bls_dataframe,
                "Courses": courses_dataframe,
            },
            share_with="user@example.com",
        )
    """

    def __init__(self, credentials_path: Optional[Path] = None):
        """
        Initialize the Google Sheets service.

        Args:
            credentials_path: Path to service account JSON. Uses settings if not provided.
        """
        self._credentials_path = credentials_path or settings.google_credentials_path
        self._client: Optional[gspread.Client] = None
        self._initialized = False

    def _get_client(self) -> gspread.Client:
        """Get or create the gspread client with credentials."""
        if self._client is None:
            if not self._credentials_path.exists():
                raise FileNotFoundError(
                    f"Google credentials file not found at: {self._credentials_path}\n"
                    "Please follow the setup instructions to create a service account "
                    "and download the credentials JSON file."
                )

            credentials = Credentials.from_service_account_file(
                str(self._credentials_path),
                scopes=SCOPES,
            )
            self._client = gspread.authorize(credentials)
            self._initialized = True
            logger.info("Google Sheets client initialized successfully")

        return self._client

    def is_available(self) -> bool:
        """Check if the service is available (credentials exist)."""
        return self._credentials_path.exists() and settings.google_drive_folder_id != ""

    def get_service_account_email(self) -> Optional[str]:
        """Get the service account email for sharing folders."""
        if not self._credentials_path.exists():
            return None

        import json
        with open(self._credentials_path) as f:
            data = json.load(f)
            return data.get("client_email")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def create_output(
        self,
        title: str,
        data: dict[str, pd.DataFrame],
        share_with: Optional[str] = None,
        sharing_mode: Literal["restricted", "anyone"] = "restricted",
        notify: bool = False,
    ) -> dict:
        """
        Create a new Google Spreadsheet with the provided data.

        Args:
            title: Title for the spreadsheet
            data: Dictionary mapping sheet names to DataFrames
            share_with: Email address to share with (for restricted mode)
            sharing_mode: "restricted" (share with specific email) or "anyone" (anyone with link)
            notify: Whether to send email notification when sharing

        Returns:
            Dictionary with:
                - spreadsheet_id: The spreadsheet ID
                - spreadsheet_url: URL to the spreadsheet
                - folder_url: URL to the containing folder
                - shared_with: Email address(es) shared with
        """
        from googleapiclient.discovery import build

        # Generate timestamped title
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        full_title = f"{title} - {timestamp}"

        logger.info(f"Creating spreadsheet: {full_title}")

        # Get credentials for direct API access
        credentials = Credentials.from_service_account_file(
            str(self._credentials_path),
            scopes=SCOPES,
        )

        # Use Drive API to create file directly in the target folder
        drive_service = build("drive", "v3", credentials=credentials)

        file_metadata = {
            "name": full_title,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [settings.google_drive_folder_id],  # Create directly in folder
        }

        file = drive_service.files().create(
            body=file_metadata,
            fields="id, webViewLink",
        ).execute()

        spreadsheet_id = file.get("id")
        spreadsheet_url = file.get("webViewLink", f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}")

        logger.info(f"Spreadsheet created with ID: {spreadsheet_id}")

        # Now open it with gspread to write data
        client = self._get_client()
        spreadsheet = client.open_by_key(spreadsheet_id)

        # Write data to sheets
        self._write_data_to_sheets(spreadsheet, data)

        # Set sharing permissions
        shared_with_list = []
        if sharing_mode == "anyone":
            spreadsheet.share("", perm_type="anyone", role="reader")
            shared_with_list.append("Anyone with link")
            logger.info("Shared with anyone who has the link")
        elif share_with:
            spreadsheet.share(
                share_with,
                perm_type="user",
                role="reader",
                notify=notify,
            )
            shared_with_list.append(share_with)
            logger.info(f"Shared with: {share_with}")

        return {
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_url": spreadsheet_url,
            "folder_url": f"https://drive.google.com/drive/folders/{settings.google_drive_folder_id}",
            "shared_with": shared_with_list,
            "title": full_title,
        }

    def _write_data_to_sheets(
        self,
        spreadsheet: gspread.Spreadsheet,
        data: dict[str, pd.DataFrame],
    ) -> None:
        """Write DataFrames to sheets, creating tabs as needed."""
        # Get the first sheet (always exists) and track if we've used it
        first_sheet = spreadsheet.sheet1
        first_sheet_used = False

        for sheet_name, df in data.items():
            if df.empty:
                logger.warning(f"Skipping empty DataFrame for sheet: {sheet_name}")
                continue

            # Use the first sheet for the first data, create new sheets for others
            if not first_sheet_used:
                worksheet = first_sheet
                worksheet.update_title(sheet_name)
                first_sheet_used = True
            else:
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=len(df) + 1,
                    cols=len(df.columns),
                )

            # Convert DataFrame to list of lists (header + data)
            # Handle NaN values and convert to strings
            df_clean = df.fillna("")
            for col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str)

            data_to_write = [df.columns.tolist()] + df_clean.values.tolist()

            # Write all data at once (more efficient than cell-by-cell)
            worksheet.update(
                range_name="A1",
                values=data_to_write,
            )

            # Format header row (bold)
            worksheet.format("1:1", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
            })

            # Auto-resize columns (approximate)
            # gspread doesn't have native auto-resize, so we set a reasonable width
            # based on the data

            logger.info(f"Wrote {len(df)} rows to sheet: {sheet_name}")

        # Delete the first sheet if it wasn't used (no data was provided)
        if not first_sheet_used:
            logger.warning("No data to write, spreadsheet will have empty first sheet")

    def _move_to_folder(self, spreadsheet_id: str, folder_id: str) -> None:
        """Move a spreadsheet to a specific Drive folder."""
        client = self._get_client()

        # Get the Drive service through gspread's client
        # We need to use the underlying Google API for moving files
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials

        credentials = Credentials.from_service_account_file(
            str(self._credentials_path),
            scopes=SCOPES,
        )
        drive_service = build("drive", "v3", credentials=credentials)

        # Get current parents
        file = drive_service.files().get(
            fileId=spreadsheet_id,
            fields="parents",
        ).execute()
        previous_parents = ",".join(file.get("parents", []))

        # Move to new folder
        drive_service.files().update(
            fileId=spreadsheet_id,
            addParents=folder_id,
            removeParents=previous_parents,
            fields="id, parents",
        ).execute()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def append_to_sheet(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        data: pd.DataFrame,
    ) -> int:
        """
        Append data to an existing sheet.

        Args:
            spreadsheet_id: ID of the spreadsheet
            sheet_name: Name of the sheet/tab to append to
            data: DataFrame to append

        Returns:
            Number of rows appended
        """
        client = self._get_client()
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)

        if data.empty:
            return 0

        # Convert DataFrame to list of lists
        df_clean = data.fillna("")
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)

        rows_to_append = df_clean.values.tolist()
        worksheet.append_rows(rows_to_append)

        logger.info(f"Appended {len(rows_to_append)} rows to {sheet_name}")
        return len(rows_to_append)

    def get_spreadsheet_info(self, spreadsheet_id: str) -> dict:
        """Get information about an existing spreadsheet."""
        client = self._get_client()
        spreadsheet = client.open_by_key(spreadsheet_id)

        return {
            "id": spreadsheet.id,
            "title": spreadsheet.title,
            "url": spreadsheet.url,
            "sheets": [ws.title for ws in spreadsheet.worksheets()],
        }


# Singleton instance for convenience
_sheets_service: Optional[GoogleSheetsService] = None


def get_sheets_service():
    """
    Get the sheets service instance.

    Prefers OAuth service (uses user's Drive quota) over service account
    (which has quota restrictions in some Google Cloud configurations).
    """
    from app.services.google_sheets_oauth import get_oauth_sheets_service, CREDENTIALS_PATH

    # Use OAuth service if credentials exist
    if CREDENTIALS_PATH.exists():
        return get_oauth_sheets_service()

    # Fall back to service account
    global _sheets_service
    if _sheets_service is None:
        _sheets_service = GoogleSheetsService()
    return _sheets_service
