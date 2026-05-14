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

SOURCE_NAME = "CITYAM"
SCRAPER_ID = 37
COMPANY_ID = os.getenv("MIDDLESEX_PARTNERSHIP_COMPANY_ID")

SITEMAP_INDEX = "https://www.cityam.com/sitemap.xml"
PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml, text/xml, */*",
    "Accept-Language": "en-GB,en;q=0.9",
}

# Namespaces used in the cityam sitemap
NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
}


def parse_lastmod(lastmod_str: str) -> str:
    """Normalise lastmod to YYYY-MM-DDTHH:MM:SS (no tz offset)."""
    # e.g. "2026-05-04T15:07:32+00:00" or "2026-05-04T15:07:32Z"
    s = lastmod_str.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s[:25], fmt[:len(fmt)])
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    # Fallback: strip tz and return as-is
    return s[:19]


def fetch_sitemap() -> list[dict]:
    """Fetch the sitemap index, pick the latest daily sitemap URL, and return [{url, lastmod}]."""
    print(f"  📅 Fetching sitemap index: {SITEMAP_INDEX}")
    try:
        resp = requests.get(SITEMAP_INDEX, headers=HEADERS, proxies=PROXIES, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        locs = []
        for sitemap_el in root.findall(".//sm:sitemap", NS):
            loc = sitemap_el.find("sm:loc", NS)
            if loc is not None and loc.text:
                locs.append(loc.text.strip())
        if not locs:
            for sitemap_el in root.findall(".//sitemap"):
                loc = sitemap_el.find("loc")
                if loc is not None and loc.text:
                    locs.append(loc.text.strip())
        if not locs:
            print("  ❌ No sitemap entries found in index.")
            return []
        daily_url = locs[0]
        print(f"  📅 Latest daily sitemap: {daily_url}")
    except Exception as e:
        print(f"  ❌ Failed to fetch sitemap index: {e}")
        return []

    try:
        resp = requests.get(daily_url, headers=HEADERS, proxies=PROXIES, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for url_el in root.findall(".//sm:url", NS):
            loc = url_el.find("sm:loc", NS)
            lastmod = url_el.find("sm:lastmod", NS)
            if loc is None or not loc.text:
                continue
            items.append({
                "url": loc.text.strip(),
                "lastmod": parse_lastmod(lastmod.text) if lastmod is not None and lastmod.text else "",
            })
        if not items:
            for url_el in root.findall(".//url"):
                loc = url_el.find("loc")
                lastmod = url_el.find("lastmod")
                if loc is None or not loc.text:
                    continue
                items.append({
                    "url": loc.text.strip(),
                    "lastmod": parse_lastmod(lastmod.text) if lastmod is not None and lastmod.text else "",
                })
        return items
    except Exception as e:
        print(f"  ❌ Failed to fetch daily sitemap: {e}")
        return []


def scrape_article(url: str, max_retries: int = 3) -> dict | None:
    """Scrape a single City AM article. Returns {title, date, text} or None."""
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = cffi_requests.get(
                url,
                impersonate="chrome131",
                proxies=PROXIES,
                timeout=30,
            )
            resp.raise_for_status()
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")

            # Title
            title_el = soup.select_one("h1.article-header__title")
            title = title_el.get_text(strip=True) if title_el else ""

            # Date from <time> datetime attribute (article page, not sitemap)
            time_el = soup.select_one("time.date-time__time")
            date_str = ""
            if time_el:
                dt_attr = time_el.get("datetime", "")
                date_str = parse_lastmod(dt_attr) if dt_attr else ""

            # Article body — paragraphs inside article, excluding ads/newsletter/read-more
            article_el = soup.select_one("article.content-container")
            body_text = ""
            if article_el:
                # Remove noise blocks
                for noise in article_el.select(
                    ".notice-header, .newsletter-auto-inject, "
                    ".read-more, .social-share, script, style, "
                    ".article-header-featured-image, footer"
                ):
                    noise.decompose()
                paragraphs = [
                    p.get_text(" ", strip=True)
                    for p in article_el.find_all("p")
                    if p.get_text(strip=True)
                ]
                body_text = "\n\n".join(paragraphs)

            if not title and not body_text:
                return None

            return {"title": title, "date": date_str, "text": body_text}

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"  ❌ Failed to scrape {url}: {e}")
                return None


def main():
    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping City AM — subscription is inactive")
        return

    print("🔍 Scraping City AM...")

    # Latest timestamp stored in DB for this scraper + company
    latest_ts = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)
    print(f"🕐 Latest timestamp in DB: {latest_ts or 'None (first run)'}")

    sitemap_items = fetch_sitemap()
    print(f"  📄 {len(sitemap_items)} URL(s) in today's sitemap.")

    if not sitemap_items:
        print("⛔ No items found in sitemap.")
        return

    # Filter to only articles newer than latest_ts
    if latest_ts:
        new_items = [
            item for item in sitemap_items
            if item["lastmod"] and item["lastmod"] > latest_ts
        ]
    else:
        new_items = sitemap_items

    if not new_items:
        print("⛔ No new articles since last run.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

    articles = []
    max_date_seen = latest_ts or ""

    def scrape_one(item):
        url = item["url"]
        lastmod = item["lastmod"]
        print(f"  Scraping: {url}")
        result = scrape_article(url)
        if not result:
            return None
        date = result["date"] or lastmod
        print(f"  ✅ {result['title'][:70]}")
        return {
            "url": url,
            "title": result["title"],
            "text": result["text"],
            "date": date,
            "scraper_id": SCRAPER_ID,
            "company_id": COMPANY_ID,
            "_lastmod": lastmod,
        }

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_one, item): item for item in new_items}
        for future in as_completed(futures):
            result = future.result()
            if result:
                lm = result.pop("_lastmod", "")
                articles.append(result)
                if lm and lm > max_date_seen:
                    max_date_seen = lm

    if not articles:
        print("⛔ No articles scraped successfully.")
        return

    inserted = insert_articles(articles)
    print(f"✅ Inserted {inserted} articles into database.")

    # Update the stored timestamp to the newest lastmod seen
    if max_date_seen and max_date_seen != latest_ts:
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, max_date_seen)
        print(f"🕐 Updated latest timestamp to: {max_date_seen}")


if __name__ == "__main__":
    main()
