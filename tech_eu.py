import os
import time
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

FEED_URL = "https://tech.eu/feed/"
SOURCE_NAME = "TECH_EU"
SCRAPER_ID = 62

COMPANY_CONFIGS = [
    {
        "label": "VM Search",
        "company_id": os.getenv("VM_SEARCH_COMPANY_ID"),
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


def parse_pubdate(date_str):
    """Convert RFC 2822 pubDate to ISO 8601 string."""
    try:
        dt = parsedate_to_datetime(date_str.strip())
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def clean_html_content(html_content):
    """Convert HTML content to clean text."""
    soup = BeautifulSoup(html_content, "html.parser")
    for tag in soup.select("script, style, iframe"):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return " ".join(text.split())


def fetch_feed(max_retries=3):
    """Fetch the Tech.eu RSS feed XML."""
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(FEED_URL, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


def parse_feed(xml_content):
    """Parse RSS feed and return list of article dicts ordered newest first."""
    soup = BeautifulSoup(xml_content, "xml")
    items = soup.find_all("item")
    articles = []

    for item in items:
        title_tag = item.find("title")
        link_tag = item.find("link")
        pubdate_tag = item.find("pubDate")
        content_tag = item.find("content:encoded")
        desc_tag = item.find("description")

        title = title_tag.get_text(strip=True) if title_tag else ""
        url = link_tag.get_text(strip=True) if link_tag else ""
        pubdate_raw = pubdate_tag.get_text(strip=True) if pubdate_tag else ""
        timestamp = parse_pubdate(pubdate_raw)

        if not url or not timestamp:
            continue

        raw_html = ""
        if content_tag and content_tag.get_text(strip=True):
            raw_html = content_tag.get_text(strip=True)
        elif desc_tag and desc_tag.get_text(strip=True):
            raw_html = desc_tag.get_text(strip=True)

        text = clean_html_content(raw_html) if raw_html else ""

        articles.append({
            "url": url,
            "title": title,
            "text": text,
            "date": timestamp,
            "lastmod": timestamp,
            "scraper_id": SCRAPER_ID,
        })
        print(f"  Parsed: {title[:70]}...")

    return articles


def main():
    print("🔍 Fetching articles from Tech.eu RSS feed...")

    xml_content = fetch_feed()
    if not xml_content:
        print("⛔ Failed to fetch feed.")
        return

    all_posts = parse_feed(xml_content)
    if not all_posts:
        print("⛔ No articles parsed from feed.")
        return

    print(f"📋 Parsed {len(all_posts)} article(s) from feed.")
    newest_timestamp = all_posts[0]["lastmod"] if all_posts else None

    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        label = config["label"]

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        if not is_subscription_active(SCRAPER_ID, company_id):
            print(f"⏭️  Skipping {label} — subscription is inactive")
            continue

        saved_timestamp = get_latest_timestamp(SCRAPER_ID, company_id)

        # ----------------------------
        # FIRST RUN — NO SCRAPING
        # ----------------------------
        if saved_timestamp is None:
            print("🟢 First run detected — NOT saving any articles.")
            if newest_timestamp:
                print("Saving latest timestamp:", newest_timestamp)
                update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        print("Previously saved timestamp:", saved_timestamp)

        new_articles = [a for a in all_posts if a["lastmod"] > saved_timestamp]

        if not new_articles:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_articles)} new articles.")

        company_articles = [dict(a, company_id=company_id) for a in new_articles]
        inserted_count = insert_articles(company_articles)
        print(f"✅ Inserted {inserted_count} articles for {label}")

        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
