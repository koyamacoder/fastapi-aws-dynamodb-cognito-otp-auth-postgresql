import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional
import logging

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import smtplib

from trucost.core.services.base import BaseService

if TYPE_CHECKING:
    from trucost.core.settings import MetaSettings, Metaservices

logger = logging.getLogger(__name__)


class EmailService(BaseService):
    def __init__(self):
        super().__init__()
        self._smtp = None
        self._settings = None

    async def connect(self, settings: "MetaSettings", services: "Metaservices"):
        """Connect to the SMTP server."""
        await super().connect(settings, services)
        self._settings = settings
        await self._ensure_connection()

    async def disconnect(self, settings: "MetaSettings"):
        """Disconnect from the SMTP server."""
        if self._smtp:
            try:
                self._smtp.quit()
            except Exception as e:
                logger.warning(f"Error during SMTP disconnect: {e}")
        await super().disconnect(settings)

    async def _ensure_connection(self):
        """Ensure SMTP connection is active and reconnect if needed."""
        try:
            if self._smtp:
                try:
                    status = self._smtp.noop()[0]
                except Exception:
                    status = -1

                if status != 250:
                    self._smtp = None

            if not self._smtp:
                # Create new SMTP connection
                self._smtp = smtplib.SMTP(
                    self._settings.smtp_host,
                    self._settings.smtp_port,
                    timeout=30,  # Add timeout
                )
                if self._settings.smtp_use_tls:
                    self._smtp.starttls()

                if self._settings.smtp_username and self._settings.smtp_password:
                    self._smtp.login(
                        self._settings.smtp_username, self._settings.smtp_password
                    )
        except Exception as e:
            logger.error(f"Failed to establish SMTP connection: {e}")
            raise

    def sync_send_email(
        self,
        to: str | List[str],
        subject: str,
        body: str,
        html_body: Optional[str] = None,
        attachments: Optional[List[Path]] = None,
    ):
        """Send an email with optional HTML content and attachments."""
        if not self._smtp or not self._settings:
            raise RuntimeError("Email service not connected")

        # Convert single email to list
        if isinstance(to, str):
            to = [to]

        # Create message container
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self._settings.smtp_from_email
        msg["To"] = ", ".join(to)

        # Add plain text body
        msg.attach(MIMEText(body, "plain"))

        # Add HTML body if provided
        if html_body:
            msg.attach(MIMEText(html_body, "html"))

        # Add attachments if provided
        if attachments:
            for attachment in attachments:
                with open(attachment, "rb") as f:
                    part = MIMEApplication(f.read(), Name=attachment.name)
                    part["Content-Disposition"] = (
                        f'attachment; filename="{attachment.name}"'
                    )
                    msg.attach(part)

        # Send email with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._smtp.send_message(msg)
                break
            except (smtplib.SMTPServerDisconnected, smtplib.SMTPSenderRefused) as e:
                logger.warning(f"SMTP error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    # Force reconnection
                    self._smtp = None
                    asyncio.run(self._ensure_connection())
                else:
                    raise

    async def send_email(
        self,
        to: str | List[str],
        subject: str,
        body: str,
        html_body: Optional[str] = None,
        attachments: Optional[List[Path]] = None,
    ):
        """Send an email with optional HTML content and attachments."""
        await asyncio.to_thread(
            self.sync_send_email, to, subject, body, html_body, attachments
        )
