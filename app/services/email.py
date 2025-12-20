"""
Email Service - Send notification emails with results links.

Supports:
- Gmail SMTP with App Password
- Async email sending
- HTML email templates
"""

import logging
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional

from config.settings import settings

logger = logging.getLogger(__name__)


class EmailService:
    """
    Service for sending email notifications.

    Uses Gmail SMTP with App Password authentication.
    """

    def __init__(
        self,
        sender_email: Optional[str] = None,
        app_password: Optional[str] = None,
        smtp_host: Optional[str] = None,
        smtp_port: Optional[int] = None,
    ):
        """
        Initialize the email service.

        Args:
            sender_email: Sender email address (uses settings if not provided)
            app_password: Gmail App Password (uses settings if not provided)
            smtp_host: SMTP server host (default: smtp.gmail.com)
            smtp_port: SMTP server port (default: 587)
        """
        self._sender = sender_email or settings.email_sender
        self._password = app_password or settings.email_app_password
        self._smtp_host = smtp_host or settings.email_smtp_host
        self._smtp_port = smtp_port or settings.email_smtp_port

    def is_available(self) -> bool:
        """Check if email service is configured."""
        return bool(self._sender and self._password)

    def send_results_email(
        self,
        to_email: str,
        topic: str,
        spreadsheet_url: str,
        folder_url: str,
        run_summary: dict,
    ) -> bool:
        """
        Send email with results link.

        Args:
            to_email: Recipient email address
            topic: Topic/title of the run
            spreadsheet_url: URL to the Google Spreadsheet
            folder_url: URL to the Google Drive folder
            run_summary: Summary of the run (modules, status, etc.)

        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.is_available():
            logger.warning("Email service not configured, skipping notification")
            return False

        try:
            # Create message
            message = MIMEMultipart("alternative")
            message["Subject"] = f"Market Intelligence Results: {topic}"
            message["From"] = self._sender
            message["To"] = to_email

            # Create HTML content
            html_content = self._create_html_email(
                topic=topic,
                spreadsheet_url=spreadsheet_url,
                folder_url=folder_url,
                run_summary=run_summary,
            )

            # Create plain text fallback
            text_content = self._create_text_email(
                topic=topic,
                spreadsheet_url=spreadsheet_url,
                folder_url=folder_url,
                run_summary=run_summary,
            )

            # Attach parts
            part1 = MIMEText(text_content, "plain")
            part2 = MIMEText(html_content, "html")
            message.attach(part1)
            message.attach(part2)

            # Send email
            context = ssl.create_default_context()

            with smtplib.SMTP(self._smtp_host, self._smtp_port) as server:
                server.starttls(context=context)
                server.login(self._sender, self._password)
                server.sendmail(self._sender, to_email, message.as_string())

            logger.info(f"Results email sent to {to_email}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def _create_html_email(
        self,
        topic: str,
        spreadsheet_url: str,
        folder_url: str,
        run_summary: dict,
    ) -> str:
        """Create HTML email content."""
        # Build module results table
        modules_html = ""
        for module_name, module_info in run_summary.get("modules", {}).items():
            status = module_info.get("status", "unknown")
            status_color = {
                "completed": "#28a745",
                "partial": "#ffc107",
                "failed": "#dc3545",
            }.get(status, "#6c757d")

            status_emoji = {
                "completed": "&#10004;",  # checkmark
                "partial": "&#9888;",  # warning
                "failed": "&#10006;",  # X
            }.get(status, "&#8226;")

            modules_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;">{module_info.get('display_name', module_name)}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; color: {status_color};">
                    {status_emoji} {status.capitalize()}
                </td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;">{module_info.get('rows', 0)} rows</td>
            </tr>
            """

        timestamp = datetime.now().strftime("%B %d, %Y at %I:%M %p")

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #8C1D40 0%, #FFC627 100%); padding: 20px; border-radius: 8px 8px 0 0;">
                <h1 style="color: white; margin: 0; font-size: 24px;">Market Intelligence Results</h1>
            </div>

            <div style="background: #fff; padding: 20px; border: 1px solid #ddd; border-top: none; border-radius: 0 0 8px 8px;">
                <h2 style="color: #8C1D40; margin-top: 0;">{topic}</h2>

                <p>Your market intelligence analysis is complete. Here are the results:</p>

                <div style="margin: 20px 0;">
                    <a href="{spreadsheet_url}"
                       style="display: inline-block; background: #8C1D40; color: white; padding: 12px 24px; text-decoration: none; border-radius: 4px; font-weight: bold;">
                        View Results in Google Sheets
                    </a>
                </div>

                <h3 style="color: #333; border-bottom: 2px solid #FFC627; padding-bottom: 8px;">Run Summary</h3>

                <table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
                    <thead>
                        <tr style="background: #f8f9fa;">
                            <th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">Module</th>
                            <th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">Status</th>
                            <th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">Data</th>
                        </tr>
                    </thead>
                    <tbody>
                        {modules_html}
                    </tbody>
                </table>

                <p style="color: #666; font-size: 14px;">
                    <strong>Generated:</strong> {timestamp}<br>
                    <strong>All results folder:</strong> <a href="{folder_url}" style="color: #8C1D40;">View in Google Drive</a>
                </p>

                <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;">

                <p style="color: #999; font-size: 12px;">
                    This is an automated message from the ASU Learning Enterprise Market Intelligence Tool.
                </p>
            </div>
        </body>
        </html>
        """
        return html

    def _create_text_email(
        self,
        topic: str,
        spreadsheet_url: str,
        folder_url: str,
        run_summary: dict,
    ) -> str:
        """Create plain text email content."""
        timestamp = datetime.now().strftime("%B %d, %Y at %I:%M %p")

        # Build module summary
        modules_text = ""
        for module_name, module_info in run_summary.get("modules", {}).items():
            status = module_info.get("status", "unknown")
            modules_text += f"  - {module_info.get('display_name', module_name)}: {status.capitalize()} ({module_info.get('rows', 0)} rows)\n"

        text = f"""
Market Intelligence Results: {topic}
{'=' * 50}

Your market intelligence analysis is complete.

VIEW RESULTS:
{spreadsheet_url}

RUN SUMMARY:
{modules_text}

Generated: {timestamp}
All results folder: {folder_url}

---
ASU Learning Enterprise Market Intelligence Tool
        """
        return text.strip()


# Singleton instance
_email_service: Optional[EmailService] = None


def get_email_service() -> EmailService:
    """Get the singleton email service instance."""
    global _email_service
    if _email_service is None:
        _email_service = EmailService()
    return _email_service
