"""
Unit tests for Google Sheets Service.
"""
import pytest
from unittest.mock import MagicMock, patch, call
import pandas as pd

from app.services.google_sheets import GoogleSheetsService


class TestGoogleSheetsService:
    """Test Google Sheets Service functionality."""

    @pytest.fixture
    def mock_credentials(self):
        """Mock Google credentials."""
        with patch('app.services.google_sheets.Credentials') as mock_creds:
            mock_instance = MagicMock()
            mock_creds.from_service_account_info.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def sheets_service(self, mock_credentials):
        """Create sheets service with mocked credentials."""
        with patch('app.services.google_sheets.settings') as mock_settings:
            mock_settings.google_credentials_path = "/path/to/creds.json"
            mock_settings.google_drive_folder_id = "test_folder_id"
            mock_settings.google_available = True

            with patch('builtins.open', create=True) as mock_open:
                mock_open.return_value.__enter__.return_value.read.return_value = '{"type": "service_account"}'

                with patch('app.services.google_sheets.build') as mock_build, \
                     patch('app.services.google_sheets.gspread.authorize') as mock_gspread:

                    mock_drive = MagicMock()
                    mock_build.return_value = mock_drive

                    mock_gspread_client = MagicMock()
                    mock_gspread.return_value = mock_gspread_client

                    service = GoogleSheetsService()
                    service._drive_service = mock_drive
                    service._gspread_client = mock_gspread_client

                    return service

    def test_is_available(self, sheets_service):
        """Test service availability check."""
        assert sheets_service.is_available() is True

    def test_create_output_success(self, sheets_service, sample_dataframe):
        """Test successful spreadsheet creation."""
        # Mock Drive API file creation
        mock_file_response = {
            "id": "spreadsheet_123",
            "webViewLink": "https://docs.google.com/spreadsheets/d/spreadsheet_123"
        }
        sheets_service._drive_service.files().create().execute.return_value = mock_file_response

        # Mock gspread operations
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet
        sheets_service._gspread_client.open_by_key.return_value = mock_spreadsheet

        # Mock sharing
        mock_spreadsheet.share = MagicMock()

        data = {"Sheet1": sample_dataframe}

        result = sheets_service.create_output(
            title="Test Spreadsheet",
            data=data,
            share_with="user@example.com",
            sharing_mode="restricted"
        )

        assert result["spreadsheet_id"] == "spreadsheet_123"
        assert result["spreadsheet_url"] == "https://docs.google.com/spreadsheets/d/spreadsheet_123"
        assert "user@example.com" in result["shared_with"]

    def test_create_output_writer_role(self, sheets_service, sample_dataframe):
        """Test that user gets writer (editor) access."""
        mock_file_response = {
            "id": "spreadsheet_123",
            "webViewLink": "https://docs.google.com/spreadsheets/d/spreadsheet_123"
        }
        sheets_service._drive_service.files().create().execute.return_value = mock_file_response

        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        sheets_service._gspread_client.open_by_key.return_value = mock_spreadsheet

        data = {"Sheet1": sample_dataframe}

        sheets_service.create_output(
            title="Test Spreadsheet",
            data=data,
            share_with="user@example.com",
            sharing_mode="restricted"
        )

        # Verify share was called with writer role
        mock_spreadsheet.share.assert_called()
        call_args = mock_spreadsheet.share.call_args
        assert call_args[1]["role"] == "writer"  # Not "reader"

    def test_create_output_file_creation_failure(self, sheets_service, sample_dataframe):
        """Test handling of file creation failure."""
        # Mock file creation failure
        sheets_service._drive_service.files().create().execute.side_effect = Exception("Drive API Error")

        data = {"Sheet1": sample_dataframe}

        with pytest.raises(RuntimeError) as exc_info:
            sheets_service.create_output(
                title="Test Spreadsheet",
                data=data,
                share_with="user@example.com"
            )

        assert "Failed to create spreadsheet in Google Drive" in str(exc_info.value)

    def test_create_output_data_write_failure(self, sheets_service, sample_dataframe):
        """Test that data write failure doesn't prevent spreadsheet creation."""
        mock_file_response = {
            "id": "spreadsheet_123",
            "webViewLink": "https://docs.google.com/spreadsheets/d/spreadsheet_123"
        }
        sheets_service._drive_service.files().create().execute.return_value = mock_file_response

        # Mock data write failure
        sheets_service._gspread_client.open_by_key.side_effect = Exception("Write failed")

        data = {"Sheet1": sample_dataframe}

        # Should not raise exception - just log warning
        result = sheets_service.create_output(
            title="Test Spreadsheet",
            data=data,
            share_with="user@example.com"
        )

        # Spreadsheet should still be created
        assert result["spreadsheet_id"] == "spreadsheet_123"

    def test_create_output_share_failure(self, sheets_service, sample_dataframe):
        """Test that sharing failure doesn't prevent spreadsheet creation."""
        mock_file_response = {
            "id": "spreadsheet_123",
            "webViewLink": "https://docs.google.com/spreadsheets/d/spreadsheet_123"
        }
        sheets_service._drive_service.files().create().execute.return_value = mock_file_response

        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        sheets_service._gspread_client.open_by_key.return_value = mock_spreadsheet

        # Mock sharing failure
        mock_spreadsheet.share.side_effect = Exception("Share failed")

        data = {"Sheet1": sample_dataframe}

        # Should not raise exception
        result = sheets_service.create_output(
            title="Test Spreadsheet",
            data=data,
            share_with="user@example.com"
        )

        # Spreadsheet should still be created
        assert result["spreadsheet_id"] == "spreadsheet_123"

    def test_create_output_anyone_sharing(self, sheets_service, sample_dataframe):
        """Test sharing with anyone mode."""
        mock_file_response = {
            "id": "spreadsheet_123",
            "webViewLink": "https://docs.google.com/spreadsheets/d/spreadsheet_123"
        }
        sheets_service._drive_service.files().create().execute.return_value = mock_file_response

        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        sheets_service._gspread_client.open_by_key.return_value = mock_spreadsheet

        data = {"Sheet1": sample_dataframe}

        result = sheets_service.create_output(
            title="Test Spreadsheet",
            data=data,
            sharing_mode="anyone"
        )

        # Verify share was called with anyone
        mock_spreadsheet.share.assert_called()
        call_args = mock_spreadsheet.share.call_args
        assert call_args[0][0] == ""  # Empty string for anyone
        assert call_args[1]["perm_type"] == "anyone"
        assert call_args[1]["role"] == "writer"

    def test_write_data_multiple_sheets(self, sheets_service):
        """Test writing data to multiple sheets."""
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet

        data = {
            "Sheet1": pd.DataFrame({"col1": [1, 2]}),
            "Sheet2": pd.DataFrame({"col2": [3, 4]}),
            "Sheet3": pd.DataFrame({"col3": [5, 6]})
        }

        sheets_service._write_data_to_sheets(mock_spreadsheet, data)

        # Verify multiple sheets were created
        assert mock_spreadsheet.add_worksheet.call_count >= 2

    def test_write_data_empty_dataframe(self, sheets_service):
        """Test handling of empty dataframes."""
        mock_spreadsheet = MagicMock()

        data = {
            "Sheet1": pd.DataFrame(),  # Empty
            "Sheet2": pd.DataFrame({"col": [1, 2]})  # Not empty
        }

        # Should not raise exception
        sheets_service._write_data_to_sheets(mock_spreadsheet, data)

    def test_sheet_name_truncation(self, sheets_service, sample_dataframe):
        """Test that long sheet names are truncated."""
        mock_file_response = {
            "id": "spreadsheet_123",
            "webViewLink": "https://docs.google.com/spreadsheets/d/spreadsheet_123"
        }
        sheets_service._drive_service.files().create().execute.return_value = mock_file_response

        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet
        sheets_service._gspread_client.open_by_key.return_value = mock_spreadsheet

        # Create very long sheet name
        long_name = "A" * 150
        data = {long_name: sample_dataframe}

        sheets_service.create_output(
            title="Test",
            data=data,
            share_with="user@example.com"
        )

        # Verify sheet name was created (would be truncated internally to 100 chars)
        assert mock_spreadsheet.add_worksheet.called
