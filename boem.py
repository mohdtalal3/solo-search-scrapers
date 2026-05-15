import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import (
    get_latest_timestamp,
    insert_articles,
    is_subscription_active,
    update_latest_timestamp,
)

load_dotenv()

SOURCE_NAME = "BOEM"
SCRAPER_ID = 42
COMPANY_ID = os.getenv("NET_ZERO_SEARCH_COMPANY_ID")

SITEMAP_URL = "https://www.boem.gov/sitemap.xml"
BASE_URL = "https://www.boem.gov"

# Only scrape URLs under these paths
ALLOWED_PREFIXES = (
    "/newsroom/press-releases",
    "/renewable-energy/state-activities",
)

PROXIES = None

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


def parse_lastmod(lastmod_str: str) -> str:
    """Normalise lastmod to YYYY-MM-DDTHH:MM:SS (strip tz offset)."""
    s = lastmod_str.strip()
    # Remove tz offset (+HH:MM or -HH:MM or Z)
    s = s.replace("Z", "")
    # Strip +HH:MM or -HH:MM at the end
    for i in range(len(s) - 1, -1, -1):
        if s[i] in ("+", "-") and i > 10:
            s = s[:i]
            break
    return s[:19]


def fetch_sitemap_items() -> list[dict]:
    """
    Fetch BOEM sitemap (index or direct), filter by ALLOWED_PREFIXES,
    return list of {url, lastmod}.
    """
    print(f"  📡 Fetching sitemap: {SITEMAP_URL}")
    try:
        resp = requests.get(SITEMAP_URL, headers=HEADERS, proxies=PROXIES, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        print(f"  ❌ Failed to fetch sitemap: {e}")
        return []

    # Check if it's a sitemap index
    child_sitemaps = root.findall(".//sm:sitemap/sm:loc", NS)
    if not child_sitemaps:
        child_sitemaps = root.findall(".//sitemap/loc")

    if child_sitemaps:
        # Sitemap index — fetch each child and aggregate
        all_items = []
        for loc_el in child_sitemaps:
            child_url = loc_el.text.strip() if loc_el.text else ""
            if not child_url:
                continue
            child_items = _parse_url_sitemap(child_url)
            all_items.extend(child_items)
        return all_items
    else:
        # Single sitemap
        return _parse_url_sitemap_root(root)


def _parse_url_sitemap(sitemap_url: str) -> list[dict]:
    """Fetch and parse a single URL sitemap, filtering by ALLOWED_PREFIXES."""
    try:
        time.sleep(0.5)
        resp = requests.get(sitemap_url, headers=HEADERS, proxies=PROXIES, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        print(f"  ⚠️  Failed to fetch child sitemap {sitemap_url}: {e}")
        return []
    return _parse_url_sitemap_root(root)


def _parse_url_sitemap_root(root: ET.Element) -> list[dict]:
    """Parse <url> entries from a sitemap XML root, filtering by ALLOWED_PREFIXES."""
    items = []
    url_els = root.findall(".//sm:url", NS)
    if not url_els:
        url_els = root.findall(".//url")

    for url_el in url_els:
        loc_el = url_el.find("sm:loc", NS)
        if loc_el is None:
            loc_el = url_el.find("loc")
        lastmod_el = url_el.find("sm:lastmod", NS)
        if lastmod_el is None:
            lastmod_el = url_el.find("lastmod")

        if loc_el is None or not loc_el.text:
            continue

        loc = loc_el.text.strip()

        # Extract path portion for prefix matching
        path = loc.replace("https://www.boem.gov", "").replace("http://www.boem.gov", "")
        if not any(path.startswith(prefix) for prefix in ALLOWED_PREFIXES):
            continue

        lastmod = ""
        if lastmod_el is not None and lastmod_el.text:
            lastmod = parse_lastmod(lastmod_el.text)

        items.append({"url": loc, "lastmod": lastmod})

    return items


def scrape_article(url: str, lastmod: str) -> dict | None:
    """Scrape a BOEM press release or state activity page."""
    try:
        time.sleep(1)
        resp = requests.get(
            url,
            headers=HEADERS,
            proxies=PROXIES,
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"  ❌ Failed to fetch {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Title
    title_el = soup.select_one("h1.page-title__text")
    title = title_el.get_text(strip=True) if title_el else ""

    # Date — press releases have a release date field; use lastmod as fallback
    date = lastmod
    release_date_el = soup.select_one(".field--field-release-date .field__item")
    if release_date_el:
        raw = release_date_el.get_text(strip=True)  # e.g. "04/22/2026"
        try:
            dt = datetime.strptime(raw, "%m/%d/%Y")
            date = dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass

    # Body — get the main article/page content, strip noise
    body = ""
    article_el = soup.select_one("#block-boem-content article")
    if article_el:
        for tag in article_el.select(
            "script, style, .field--field-contact, .breadcrumb, "
            ".node__header, nav, iframe, .embedded-entity"
        ):
            tag.decompose()
        body = article_el.get_text(" ", strip=True)
    else:
        main_el = soup.select_one("main.l-page__main")
        if main_el:
            for tag in main_el.select("script, style, nav, iframe, .l-region--featured"):
                tag.decompose()
            body = main_el.get_text(" ", strip=True)

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
        print("⏭️  Skipping BOEM — subscription is inactive")
        return

    print("🔍 Scraping BOEM (Press Releases & Renewable Energy State Activities)...")

    latest_ts = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)
    print(f"🕒 Latest saved timestamp: {latest_ts or 'none (first run)'}")

    items = fetch_sitemap_items()
    print(f"  📰 {len(items)} matching URL(s) found in sitemap.")
    if not items:
        print("⛔ No items found.")
        return

    # Filter: only items newer than latest timestamp
    latest_ts_clean = parse_lastmod(latest_ts) if latest_ts else None
    new_items = []
    for item in items:
        if latest_ts_clean and item["lastmod"] and item["lastmod"] <= latest_ts_clean:
            continue
        new_items.append(item)

    if not new_items:
        print("⛔ No new articles since last run.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

    articles = []

    def scrape_one(item: dict):
        print(f"  Scraping: {item['url']}")
        result = scrape_article(item["url"], item["lastmod"])
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

    # Update timestamp to the newest lastmod seen
    newest_ts = max(
        (a["date"] for a in articles if a["date"]),
        default=None,
    )
    if newest_ts:
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_ts)
        print(f"🕒 Updated latest timestamp to: {newest_ts}")


if __name__ == "__main__":
    main()
