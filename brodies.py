import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from curl_cffi import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

SITEMAP_URL = "https://brodies.com/sitemap-insights.xml"
SOURCE_NAME = "BRODIES"
SCRAPER_ID = 96
MAX_THREADS = 5

HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/jxl,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-language": "en-US,en;q=0.9,fr;q=0.8,af;q=0.7,ar;q=0.6,be;q=0.5,de;q=0.4",
    "cache-control": "max-age=0",
    "sec-ch-ua": '"Not:A-Brand";v="8", "Chromium";v="156", "Google Chrome";v="156"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/156.0.0.0 Safari/537.36"
    ),
}

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

COMPANY_CONFIGS = [
    {
        "label": "Inglis Legal Recruitment",
        "company_id": os.getenv("INGLIS_LEGAL_RECRUITMENT_COMPANY_ID"),
    },
]

BASE_URL = "https://brodies.com"

# Session with the rotating proxy — a warm-up request to the homepage
# establishes the WAF cookies (waf_token etc.) reused by all later requests.
session = requests.Session(proxies=PROXIES)
_session_ready = False


def init_session():
    """Send one request to the homepage to create the session / WAF cookies."""
    global _session_ready
    if _session_ready:
        return
    try:
        resp = session.get(
            f"{BASE_URL}/",
            headers=HEADERS,
            impersonate="chrome131",
            timeout=60,
        )
        print(f"🍪 Session warm-up ({BASE_URL}): HTTP {resp.status_code}")
    except Exception as e:
        print(f"⚠️  Session warm-up failed: {e}")
    _session_ready = True


def fetch_url(url, max_retries=3):
    """Fetch a URL via the session (Chrome impersonation + proxy)."""
    init_session()
    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = session.get(
                url,
                headers=HEADERS,
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
    """Normalise '2026-11-12T14:17:01+00:00' / '+01:00' → UTC '2026-11-12T14:17:01'."""
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return ts_str.split(".")[0]


def parse_sitemap(xml_content):
    """Return list of (loc, lastmod_utc) sorted newest first."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"❌ XML parse error: {e}")
        return []

    entries = []
    for url_el in root.findall("{*}url"):
        loc_el = url_el.find("{*}loc")
        lastmod_el = url_el.find("{*}lastmod")
        if loc_el is None or lastmod_el is None:
            continue
        entries.append((loc_el.text.strip(), normalise_ts(lastmod_el.text.strip())))

    # Sort newest first
    entries.sort(key=lambda x: x[1], reverse=True)
    return entries


def scrape_article(url, lastmod):
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title — <title> tag, strip site suffix
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""
    for suffix in (" | Brodies LLP", " - Brodies LLP"):
        if title.endswith(suffix):
            title = title[: -len(suffix)].strip()
    if not title:
        print(f"⚠️  No title found, skipping: {url}")
        return None

    # Body — the text block inside article#article__inner
    content = soup.select_one("article#article__inner .cb-text-block")
    if not content:
        return None

    for tag in content.select("script, style, iframe, form, section.cb-contributors"):
        tag.decompose()

    text = content.get_text(" ", strip=True)
    text = " ".join(text.split())
    if not text:
        return None

    return {
        "url": url,
        "date": lastmod,
        "title": title,
        "text": text,
        "lastmod": lastmod,
        "scraper_id": SCRAPER_ID,
    }


def main():
    # Exit before fetching anything if no company has an active subscription
    active_configs = [
        c for c in COMPANY_CONFIGS
        if is_subscription_active(SCRAPER_ID, c["company_id"])
    ]
    if not active_configs:
        print("⏭️  All subscriptions inactive — skipping scraper entirely")
        return

    print(f"🔍 Fetching insights sitemap: {SITEMAP_URL}")
    xml_content = fetch_url(SITEMAP_URL)
    if not xml_content:
        print("❌ Could not fetch sitemap.")
        return

    entries = parse_sitemap(xml_content)
    if not entries:
        print("❌ No entries found in sitemap.")
        return

    print(f"📋 Found {len(entries)} entries in sitemap.")

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
