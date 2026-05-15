import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests
from dotenv import load_dotenv

from db import (
    get_latest_timestamp,
    insert_articles,
    is_subscription_active,
    update_latest_timestamp,
)

load_dotenv()

SOURCE_NAME = "HEATMAP_NEWS"
SCRAPER_ID = 40
COMPANY_ID = os.getenv("NET_ZERO_SEARCH_COMPANY_ID")

SITEMAP_URL = "https://heatmap.news/feeds/sitemaps/news_1.xml"
PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
}


def parse_date(date_str: str) -> str:
    """Normalise ISO date to YYYY-MM-DDTHH:MM:SS (strip tz offset)."""
    s = date_str.strip()
    # Handle +00:00 or Z suffix
    s = s.replace("Z", "+00:00")
    # Strip tz offset — keep first 19 chars
    return s[:19]


def fetch_sitemap() -> list[dict]:
    """Fetch the news sitemap and return list of {url, title, date}."""
    print(f"  📡 Fetching sitemap: {SITEMAP_URL}")
    try:
        resp = requests.get(SITEMAP_URL, headers=HEADERS, proxies=PROXIES, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        print(f"  ❌ Failed to fetch sitemap: {e}")
        return []

    items = []
    for url_el in root.findall(".//sm:url", NS):
        loc_el = url_el.find("sm:loc", NS)
        lastmod_el = url_el.find("sm:lastmod", NS)
        title_el = url_el.find(".//news:title", NS)
        pub_date_el = url_el.find(".//news:publication_date", NS)

        loc = loc_el.text.strip() if loc_el is not None and loc_el.text else ""
        if not loc:
            continue

        date = ""
        if pub_date_el is not None and pub_date_el.text:
            date = parse_date(pub_date_el.text)
        elif lastmod_el is not None and lastmod_el.text:
            date = parse_date(lastmod_el.text)

        title = title_el.text.strip() if title_el is not None and title_el.text else ""

        items.append({"url": loc, "title": title, "date": date})

    print(f"  📰 {len(items)} item(s) found in sitemap.")
    return items


def scrape_article(url: str) -> str:
    """Scrape article body text from a Heatmap News article page."""
    try:
        time.sleep(1)
        resp = cffi_requests.get(
            url,
            impersonate="chrome131",
            proxies=PROXIES,
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"  ❌ Failed to fetch article {url}: {e}")
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    body_el = soup.select_one("div.body")
    if not body_el:
        return ""

    # Remove noise: regwall, ads, share buttons, scripts, styles
    for tag in body_el.select(
        "script, style, .regwall-container, .middle_leaderboard, "
        ".share-tab-img, .share-media-panel, .widget__shares, "
        ".snark-line, .photo-credit, .image-media"
    ):
        tag.decompose()

    return body_el.get_text(" ", strip=True)


def main():
    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping Heatmap News — subscription is inactive")
        return

    print("🔍 Scraping Heatmap News...")

    latest_ts = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)
    print(f"🕒 Latest saved timestamp: {latest_ts or 'none (first run)'}")

    items = fetch_sitemap()
    if not items:
        print("⛔ No items found in sitemap.")
        return

    # Filter to only new articles (newer than last saved timestamp)
    new_items = []
    for item in items:
        if latest_ts and item["date"] <= latest_ts:
            continue
        new_items.append(item)

    if not new_items:
        print("⛔ No new articles since last run.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

    articles = []
    newest_ts = None

    def scrape_one(item: dict):
        print(f"  Scraping: {item['url']}")
        body = scrape_article(item["url"])
        if not body and not item["title"]:
            return None
        print(f"  ✅ {item['title'][:70]}")
        return {
            "url": item["url"],
            "title": item["title"],
            "text": body,
            "date": item["date"],
            "scraper_id": SCRAPER_ID,
            "company_id": COMPANY_ID,
        }

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_one, item): item for item in new_items}
        for future in as_completed(futures):
            result = future.result()
            if result:
                articles.append(result)

    if not articles:
        print("⛔ No articles scraped successfully.")
        return

    inserted = insert_articles(articles)
    print(f"✅ Inserted {inserted} articles into database.")

    # Update timestamp to the newest article date seen
    newest_ts = max(a["date"] for a in articles if a["date"])
    if newest_ts:
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_ts)
        print(f"🕒 Updated latest timestamp to: {newest_ts}")


if __name__ == "__main__":
    main()
