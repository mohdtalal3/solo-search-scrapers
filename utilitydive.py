import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

SOURCE_NAME = "UTILITYDIVE"
SCRAPER_ID = 41
COMPANY_ID = os.getenv("NET_ZERO_SEARCH_COMPANY_ID")

SITEMAP_INDEX = "https://www.utilitydive.com/sitemap.xml"
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

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Matches archive sitemaps like /news/archive/2026/may.xml
ARCHIVE_RE = re.compile(r"/news/archive/(\d{4})/(\w+)\.xml$")

MONTH_ORDER = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def fetch_xml(url: str) -> ET.Element | None:
    try:
        resp = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)
        resp.raise_for_status()
        return ET.fromstring(resp.content)
    except Exception as e:
        print(f"  ❌ Failed to fetch XML {url}: {e}")
        return None


def get_latest_archive_sitemap() -> str | None:
    """Fetch sitemap index and return the URL of the most recent monthly archive."""
    print(f"  📡 Fetching sitemap index: {SITEMAP_INDEX}")
    root = fetch_xml(SITEMAP_INDEX)
    if root is None:
        return None

    # Collect all archive sitemap locs
    archive_locs = []
    for loc_el in root.findall(".//sm:loc", NS):
        if loc_el.text and ARCHIVE_RE.search(loc_el.text):
            archive_locs.append(loc_el.text.strip())
    # Also try without namespace
    if not archive_locs:
        for loc_el in root.findall(".//loc"):
            if loc_el.text and ARCHIVE_RE.search(loc_el.text):
                archive_locs.append(loc_el.text.strip())

    if not archive_locs:
        print("  ❌ No archive sitemaps found in index.")
        return None

    def sort_key(url: str):
        m = ARCHIVE_RE.search(url)
        if not m:
            return (0, 0)
        year = int(m.group(1))
        month = MONTH_ORDER.get(m.group(2).lower(), 0)
        return (year, month)

    latest = sorted(archive_locs, key=sort_key, reverse=True)[0]
    print(f"  📅 Latest archive sitemap: {latest}")
    return latest


def parse_archive_sitemap(sitemap_url: str) -> list[dict]:
    """Parse a monthly archive sitemap and return [{url, date}]."""
    root = fetch_xml(sitemap_url)
    if root is None:
        return []

    items = []
    for url_el in root.findall(".//sm:url", NS):
        loc_el = url_el.find("sm:loc", NS)
        lastmod_el = url_el.find("sm:lastmod", NS)
        if loc_el is None or not loc_el.text:
            continue
        loc = loc_el.text.strip()
        # Only /news/ articles
        if "/news/" not in loc:
            continue
        date = ""
        if lastmod_el is not None and lastmod_el.text:
            raw = lastmod_el.text.strip()
            # lastmod is like "2026-05-15"
            date = raw if "T" in raw else raw + "T00:00:00"
        items.append({"url": loc, "date": date})

    # Fallback: no namespace
    if not items:
        for url_el in root.findall(".//url"):
            loc_el = url_el.find("loc")
            lastmod_el = url_el.find("lastmod")
            if loc_el is None or not loc_el.text:
                continue
            loc = loc_el.text.strip()
            if "/news/" not in loc:
                continue
            date = ""
            if lastmod_el is not None and lastmod_el.text:
                raw = lastmod_el.text.strip()
                date = raw if "T" in raw else raw + "T00:00:00"
            items.append({"url": loc, "date": date})

    print(f"  📰 {len(items)} article(s) found in archive sitemap.")
    return items


def scrape_article(url: str, date: str) -> dict | None:
    """Scrape a Utility Dive article page."""
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
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Title
    title_el = soup.select_one("h1.display-heading-04")
    title = title_el.get_text(strip=True) if title_el else ""

    # Date — prefer <time> or .published-info, fallback to lastmod from sitemap
    if not date:
        pub_el = soup.select_one(".published-info")
        if pub_el:
            raw = pub_el.get_text(strip=True).replace("Published", "").strip()
            try:
                dt = datetime.strptime(raw, "%B %d, %Y")
                date = dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass

    # Body
    body_el = soup.select_one("div.article-body")
    if body_el:
        for tag in body_el.select(
            "script, style, .hybrid-ad-wrapper, .text-to-speech, "
            ".reading-list, .share-buttons, .post-article-wrapper, "
            ".social-icon-list, .custom-tooltip"
        ):
            tag.decompose()
        body = body_el.get_text(" ", strip=True)
    else:
        body = ""

    if not title and not body:
        return None

    return {
        "url": url,
        "title": title,
        "text": body,
        "date": date,
        "scraper_id": SCRAPER_ID,
        "company_id": COMPANY_ID,
    }


def main():
    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping Utility Dive — subscription is inactive")
        return

    print("🔍 Scraping Utility Dive...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs in DB.")

    archive_url = get_latest_archive_sitemap()
    if not archive_url:
        print("⛔ Could not determine latest archive sitemap.")
        return

    items = parse_archive_sitemap(archive_url)
    if not items:
        print("⛔ No items found in archive sitemap.")
        return

    new_items = [item for item in items if item["url"] not in known_urls]
    if not new_items:
        print("⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

    articles = []

    def scrape_one(item: dict):
        print(f"  Scraping: {item['url']}")
        result = scrape_article(item["url"], item["date"])
        if result:
            print(f"  ✅ {result['title'][:70]}")
        return result

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


if __name__ == "__main__":
    main()
