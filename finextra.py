import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

SOURCE_NAME = "FINEXTRA"
SCRAPER_ID = 36
COMPANY_ID = os.getenv("MIDDLESEX_PARTNERSHIP_COMPANY_ID")

RSS_CHANNELS = [
    "https://www.finextra.com/rss/channel.aspx?channel=regulation",
    "https://www.finextra.com/rss/channel.aspx?channel=crime",
    "https://www.finextra.com/rss/channel.aspx?channel=payments",
    "https://www.finextra.com/rss/channel.aspx?channel=startups",
    "https://www.finextra.com/rss/channel.aspx?channel=wholesale",
    "https://www.finextra.com/rss/channel.aspx?channel=security",
    "https://www.finextra.com/rss/channel.aspx?channel=identity",
]

SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-GB,en;q=0.9",
}


def url_slug(url: str) -> str:
    # Use the article number from the URL path as slug
    # e.g. https://www.finextra.com/newsarticle/47747/some-slug → "47747"
    parts = url.rstrip("/").split("/")
    for i, part in enumerate(parts):
        if part in ("newsarticle", "videoarticle", "event-info") and i + 1 < len(parts):
            return parts[i + 1]
    return parts[-1]


def is_excluded(url: str) -> bool:
    """Skip event-info and videoarticle links."""
    return "/event-info/" in url or "/videoarticle/" in url


def parse_rss_date(pub_date: str) -> str:
    """Convert RFC 2822 date → YYYY-MM-DDTHH:MM:SS."""
    try:
        dt = parsedate_to_datetime(pub_date)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return ""


def fetch_rss(channel_url: str, max_retries: int = 3) -> list[dict]:
    """Fetch and parse an RSS channel. Returns list of {url, title, date, description}."""
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(channel_url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            items = []
            for item in root.findall(".//item"):
                link_el = item.find("link")
                title_el = item.find("title")
                desc_el = item.find("description")
                pub_el = item.find("pubDate")

                url = link_el.text.strip() if link_el is not None and link_el.text else ""
                # Strip UTM params — keep clean URL
                url = url.split("?")[0] if "?" in url else url

                if not url or is_excluded(url):
                    continue

                items.append({
                    "url": url,
                    "title": title_el.text.strip() if title_el is not None and title_el.text else "",
                    "description": desc_el.text.strip() if desc_el is not None and desc_el.text else "",
                    "date": parse_rss_date(pub_el.text.strip()) if pub_el is not None and pub_el.text else "",
                })
            return items
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} for {channel_url}: {e}")
                time.sleep(2)
            else:
                print(f"  ❌ Failed to fetch RSS {channel_url}: {e}")
                return []


def scrape_article(url: str, max_retries: int = 3) -> str:
    """Fetch article body via Scrappey. Returns body text or empty string."""
    api_key = os.getenv("SCRAPPEY_API_KEY")
    if not api_key:
        raise RuntimeError("SCRAPPEY_API_KEY not set")

    payload = {
        "cmd": "request.get",
        "requestType": "request",
        "url": url,
        "premiumProxy": True,
        "proxyCountry": "UnitedKingdom",
        "retries": 1,
        #"automaticallySolveCaptcha": True,
        # "browserActions": [
        #     {"type": "wait_for_load_state", "waitForLoadState": "networkidle"},
        #     {"type": "wait", "wait": 1500, "when": "after_captcha"},
        # ],
    }

    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.post(
                f"{SCRAPPEY_API_URL}?key={api_key}",
                json=payload,
                timeout=90,
            )
            resp.raise_for_status()
            data = resp.json()
            html = data.get("solution", {}).get("response", "")
            if not html:
                raise RuntimeError("Empty Scrappey response")

            soup = BeautifulSoup(html, "html.parser")
            body_div = soup.find("div", id="ctl00_ctl00_body_main_NewsArticle_pnlBody")
            if body_div:
                for tag in body_div.select("script, style"):
                    tag.decompose()
                return body_div.get_text(" ", strip=True)

            # Fallback: any alt-body-copy div
            fallback = soup.select_one("div.alt-body-copy")
            if fallback:
                return fallback.get_text(" ", strip=True)

            return ""

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"  ❌ Failed to scrape {url}: {e}")
                return ""


def main():
    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping Finextra — subscription is inactive")
        return

    print("🔍 Scraping Finextra RSS channels...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs in DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    # Collect all unique new items across all channels
    new_items: dict[str, dict] = {}  # url → item dict

    for channel_url in RSS_CHANNELS:
        channel = channel_url.split("channel=")[-1]
        print(f"  📡 Fetching channel: {channel}")
        items = fetch_rss(channel_url)
        print(f"     {len(items)} item(s) found.")

        for item in items:
            url = item["url"]
            if url in known_urls:
                continue
            slug = url_slug(url)
            if slug in seen_slugs:
                continue
            if url not in new_items:
                new_items[url] = item
                seen_slugs.add(slug)

    if not new_items:
        print("⛔ No new articles found.")
        return

    print(f"\n  🆕 {len(new_items)} new article(s) to scrape.")

    articles = []

    def scrape_one(url, item):
        print(f"  Scraping: {url}")
        body = scrape_article(url)
        text = body if body else item["description"]
        print(f"  ✅ {item['title'][:70]}")
        return {
            "url": url,
            "title": item["title"],
            "text": text,
            "date": item["date"],
            "scraper_id": SCRAPER_ID,
            "company_id": COMPANY_ID,
        }

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_one, url, item): url for url, item in new_items.items()}
        for future in as_completed(futures):
            result = future.result()
            if result:
                articles.append(result)

    if not articles:
        print("⛔ No articles scraped successfully.")
        return

    inserted = insert_articles(articles)
    print(f"✅ Inserted {inserted} articles into database.")


if __name__ == "__main__":
    main()
