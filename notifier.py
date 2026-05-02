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


def _upload_file(log_path: str) -> bool:
    """Upload log file to Slack using the 3-step external upload API, then delete locally."""
    _load_env()
    token = os.environ.get('SLACK_TOKEN')
    channel = os.environ.get('SLACK_CHANNEL_ID')
    if not token or not channel:
        print("⚠️  SLACK_TOKEN or SLACK_CHANNEL_ID not set — skipping file upload")
        return False
    try:
        headers = {"Authorization": f"Bearer {token}"}
        file_name = Path(log_path).name

        # Step 1: Get upload URL
        file_size = open(log_path, "rb").seek(0, 2)

        res = requests.post(
            "https://slack.com/api/files.getUploadURLExternal",
            headers=headers,
            data={"filename": file_name, "length": file_size},
            timeout=15,
        ).json()

        if not res.get("ok"):
            print(f"⚠️  getUploadURLExternal error: {res.get('error')}")
            return False

        upload_url = res["upload_url"]
        file_id = res["file_id"]

        # Step 2: Upload file content
        with open(log_path, "rb") as f:
            requests.post(upload_url, files={"file": f}, timeout=60)

        # Step 3: Complete upload and share to channel
        complete = requests.post(
            "https://slack.com/api/files.completeUploadExternal",
            headers=headers,
            json={
                "files": [{"id": file_id, "title": file_name}],
                "channel_id": channel,
                "initial_comment": "Here is the log file 📄",
            },
            timeout=15,
        ).json()

        if not complete.get("ok"):
            print(f"⚠️  completeUploadExternal error: {complete.get('error')}")
            return False

        # Delete local log file after successful upload
        try:
            os.remove(log_path)
            print(f"🗑️  Deleted local log file: {file_name}")
        except Exception as del_err:
            print(f"⚠️  Could not delete log file: {del_err}")

        return True
    except Exception as e:
        print(f"⚠️  Slack file upload failed: {e}")
        return False

        res = requests.post(
            "https://slack.com/api/files.getUploadURLExternal",
            headers=headers,
            data={"filename": file_name, "length": file_size},
            timeout=15,
        ).json()

        if not res.get("ok"):
            print(f"⚠️  getUploadURLExternal error: {res.get('error')}")
            return False

        upload_url = res["upload_url"]
        file_id = res["file_id"]

        # Step 2: Upload file content
        with open(log_path, 'rb') as f:
            requests.post(upload_url, files={"file": f}, timeout=60)

        # Step 3: Complete upload and share to channel
        complete = requests.post(
            "https://slack.com/api/files.completeUploadExternal",
            headers=headers,
            json={
                "files": [{"id": file_id, "title": file_name}],
                "channel_id": channel,
                "initial_comment": "📄 Full scraper run log attached.",
            },
            timeout=15,
        ).json()

        if not complete.get("ok"):
            print(f"⚠️  completeUploadExternal error: {complete.get('error')}")
            return False

        # Delete local log file after successful upload
        try:
            os.remove(log_path)
            print(f"🗑️  Deleted local log file: {file_name}")
        except Exception as del_err:
            print(f"⚠️  Could not delete log file: {del_err}")

        return True
    except Exception as e:
        print(f"⚠️  Slack file upload failed: {e}")
        return False


def send_run_log(log_path: str, scraper_results: list, duration_secs: float):
    """Send run summary message + upload full log file to Slack, then delete local file."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total   = len(scraper_results)
    success = sum(1 for _, s in scraper_results if s == "success")
    failed  = sum(1 for _, s in scraper_results if s == "failed")
    skipped = sum(1 for _, s in scraper_results if s == "skipped")

    mins = int(duration_secs // 60)
    secs = int(duration_secs % 60)
    duration_str = f"{mins}m {secs}s"

    status_lines = []
    for name, status in scraper_results:
        icon = "✅" if status == "success" else ("❌" if status == "failed" else "⏭️")
        status_lines.append(f"{icon} {name}")
    status_text = "\n".join(status_lines)

    header_icon = "✅" if failed == 0 else "⚠️"

    # --- Summary message via webhook ---
    _post({
        "text": f"{header_icon} Scraper run complete — {ts}",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{header_icon} Scraper Run Complete"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Time:*\n{ts}"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n{duration_str}"},
                    {"type": "mrkdwn", "text": f"*Total scrapers:*\n{total}"},
                    {"type": "mrkdwn", "text": f"*✅ Success:*\n{success}"},
                    {"type": "mrkdwn", "text": f"*❌ Failed:*\n{failed}"},
                    {"type": "mrkdwn", "text": f"*⏭️ Skipped:*\n{skipped}"},
                ]
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Per-scraper status:*\n{status_text}"}
            },
        ]
    })

    # --- Upload full log file, then delete it locally ---
    _upload_file(log_path)


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
