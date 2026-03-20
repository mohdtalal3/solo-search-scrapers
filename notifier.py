#!/usr/bin/env python3
"""
Slack notifier — sends error alerts when a scraper fails.
Run directly to send a test notification.
"""
import os
import sys
import traceback
import requests
from pathlib import Path
from datetime import datetime


def _load_env():
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())


def _post(payload: dict) -> bool:
    """Send a payload to Slack. Returns True on success."""
    _load_env()
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if not webhook_url:
        print("⚠️  SLACK_WEBHOOK_URL not set — skipping Slack notification")
        return False
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        print(f"⚠️  Slack notification failed: {e}")
        return False


def notify_error(scraper_name: str, error: Exception):
    """Send a Slack alert when a scraper raises an exception."""
    tb = traceback.format_exc()
    # Truncate traceback so it fits in Slack
    if len(tb) > 2800:
        tb = tb[-2800:]
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    payload = {
        "text": f"❌ Scraper error: *{scraper_name}*",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"❌ Scraper Failed: {scraper_name}"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Scraper:*\n{scraper_name}"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{ts}"},
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n`{type(error).__name__}: {error}`"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Traceback:*\n```{tb}```"
                }
            }
        ]
    }
    _post(payload)


if __name__ == '__main__':
    """Quick test — sends a sample success message to verify the webhook works."""
    _load_env()
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if not webhook_url:
        print("❌ SLACK_WEBHOOK_URL not found in .env")
        sys.exit(1)

    print(f"🔗 Webhook: {webhook_url[:50]}...")
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ok = _post({
        "text": "🧪 *Slack Notification Test*",
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": "🧪 Slack Notification Test"}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": "*Status:*\n✅ Webhook working"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{ts}"},
                ]
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "Slack notifications are configured correctly! 🎉"}
            }
        ]
    })
    print("✅ Test sent!" if ok else "❌ Test failed")
    sys.exit(0 if ok else 1)
