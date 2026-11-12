import json
import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

from curl_cffi import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

SITEMAP_URL = "https://www.burnesspaull.com/sitemap.xml"
SOURCE_NAME = "BURNESS_PAULL"
SCRAPER_ID = 98
MAX_THREADS = 5

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

COMPANY_CONFIGS = [
    {
        "label": "Inglis Legal Recruitment",
        "company_id": os.getenv("INGLIS_LEGAL_RECRUITMENT_COMPANY_ID"),
    },
]

INSIGHTS_MARKER = "legal-insights"


def fetch_url(url, max_retries=3):
    """Fetch a URL via curl_cffi (Chrome impersonation + proxy)."""
    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = requests.get(
                url,
                headers=HEADERS,
                proxies=PROXIES,
                impersonate="chrome131",
                timeout=60,
            )
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed to fetch {url}: {e}")
                return None


def normalise_ts(ts_str):
    """Normalise '2026-11-11T12:06:59.2230000Z;' / '2026-11-11T12:06:59+00:00' → '2026-11-11T12:06:59'."""
    ts = ts_str.strip().rstrip(";Zz")
    ts = ts.split("+")[0].split(".")[0]
    return ts


def parse_sitemap(xml_content):
    """Return list of (loc, lastmod) for legal-insights URLs, sorted newest first."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"❌ XML parse error: {e}")
        return []

    entries = []
    for url_el in root.findall("{*}url"):
        loc_el = url_el.find("{*}loc")
        if loc_el is None:
            continue
        url = loc_el.text.strip()
        # Only insight/news article pages, not the listing pages themselves
        if INSIGHTS_MARKER not in url or url.rstrip("/").endswith(
            ("legal-insights-news-events", "legal-insights-news-events/insights")
        ):
            continue
        lastmod_el = url_el.find("{*}lastmod")
        lastmod = lastmod_el.text.strip() if lastmod_el is not None else ""
        entries.append((url, normalise_ts(lastmod)))

    # Sort newest first
    entries.sort(key=lambda x: x[1], reverse=True)
    return entries


def scrape_article(url, lastmod):
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue

        if isinstance(data, list):
            entries = data
        else:
            entries = [data]

        for entry in entries:
            if entry.get("@type") == "NewsArticle" and entry.get("articleBody"):
                date = normalise_ts(entry.get("datePublished") or lastmod)
                return {
                    "url": url,
                    "date": date,
                    "title": entry.get("headline", ""),
                    "text": entry.get("articleBody", ""),
                    "lastmod": lastmod,
                    "scraper_id": SCRAPER_ID,
                }

    print(f"⚠️  Could not find NewsArticle JSON-LD for {url}")
    return None


def main():
    # Exit before fetching anything if no company has an active subscription
    active_configs = [
        c for c in COMPANY_CONFIGS
        if is_subscription_active(SCRAPER_ID, c["company_id"])
    ]
    if not active_configs:
        print("⏭️  All subscriptions inactive — skipping scraper entirely")
        return

    print(f"🔍 Fetching sitemap: {SITEMAP_URL}")
    xml_content = fetch_url(SITEMAP_URL)
    if not xml_content:
        print("❌ Could not fetch sitemap.")
        return

    entries = parse_sitemap(xml_content)
    if not entries:
        print("❌ No legal-insights entries found in sitemap.")
        return

    print(f"📋 Found {len(entries)} legal-insights entries in sitemap.")

    newest_timestamp = entries[0][1] if entries else None

    # Collect saved timestamps for every active company
    company_timestamps = {}
    for config in active_configs:
        company_id = config["company_id"]
        company_timestamps[company_id] = {
            "label": config["label"],
            "saved_timestamp": get_latest_timestamp(SCRAPER_ID, company_id),
        }

    # Determine which URLs need scraping across all active companies
    urls_to_scrape = set()
    for company_id, info in company_timestamps.items():
        ts = info["saved_timestamp"]
        if ts is not None:
            for loc, lastmod in entries:
                if lastmod > ts:
                    urls_to_scrape.add(loc)

    # Scrape each unique article once (threaded, max 5 workers)
    scraped_cache = {}
    if urls_to_scrape:
        lastmod_by_url = {loc: lastmod for loc, lastmod in entries}
        print(f"🔎 Scraping {len(urls_to_scrape)} unique article(s) with {MAX_THREADS} threads...")

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            future_to_url = {
                executor.submit(scrape_article, url, lastmod_by_url[url]): url
                for url in urls_to_scrape
            }
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    article = future.result()
                except Exception as e:
                    print(f"  ⚠️  Exception scraping {url}: {e}")
                    continue
                if article:
                    scraped_cache[url] = article
                    print(f"  ✅ {url}")
                else:
                    print(f"  ⛔ Failed to scrape: {url}")

    # Insert scraped articles for each active company
    for company_id, info in company_timestamps.items():
        label = info["label"]
        saved_timestamp = info["saved_timestamp"]

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        # ----------------------------
        # FIRST RUN — NO SCRAPING
        # ----------------------------
        if saved_timestamp is None:
            print("🟢 First run detected — NOT scraping any articles.")
            if newest_timestamp:
                print("Saving latest timestamp:", newest_timestamp)
                update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        # ----------------------------
        # SUBSEQUENT RUNS — save new
        # ----------------------------
        print("Previously saved timestamp:", saved_timestamp)

        new_entries = [
            (loc, lastmod)
            for loc, lastmod in entries
            if lastmod > saved_timestamp
        ]

        if not new_entries:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_entries)} new articles.")

        company_articles = [
            dict(cached, company_id=company_id)
            for loc, _ in new_entries
            if (cached := scraped_cache.get(loc))
        ]

        if company_articles:
            inserted_count = insert_articles(company_articles)
            print(f"✅ Inserted {inserted_count} articles for {label}")

        if newest_timestamp:
            update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
