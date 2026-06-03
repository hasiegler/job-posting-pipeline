"""
Telegram alert transport for the data pipeline.

Single entry point: ``send_alert(message)``.  Reads ``TELEGRAM_BOT_TOKEN`` and
``TELEGRAM_CHAT_ID`` from the environment.  Never raises — any HTTP / config
failure is logged and swallowed so an alerting outage cannot fail a DAG run.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT_S = 10


def send_alert(message: str) -> None:
    """Post ``message`` to the configured Telegram chat.  Best-effort, never raises.

    No-ops silently when ``TELEGRAM_BOT_TOKEN`` or ``TELEGRAM_CHAT_ID`` is unset
    so local/dev runs without those env vars don't error.
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logger.debug(
            "send_alert: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID unset — skipping alert."
        )
        return

    try:
        resp = requests.post(
            _TELEGRAM_API.format(token=token),
            json={"chat_id": chat_id, "text": message},
            timeout=_TIMEOUT_S,
        )
        if not resp.ok:
            logger.warning(
                "send_alert: Telegram returned HTTP %s: %s",
                resp.status_code, resp.text[:200],
            )
    except Exception as e:  # noqa: BLE001 — alerting must never fail the pipeline
        logger.warning(
            "send_alert: failed to deliver alert (%s: %s)",
            e.__class__.__name__, e,
        )
