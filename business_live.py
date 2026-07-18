import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from calendar import monthrange

from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

SOURCE_NAME = "BUSINESS_LIVE"
SCRAPER_ID = 69
SITEMAP_BASE = "https://www.business-live.co.uk/sitemaps/map_art"

COMPANY_CONFIGS = [
    {
        "label": "1492 Search",
        "company_id": os.getenv("1492_SEARCH_COMPANY_ID"),
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

MAX_WORKERS = 5
MAX_RETRIES = 3


def get_proxies():
    proxy = os.getenv("SCRAPER_PROXY")
    return {"http": proxy, "https": proxy} if proxy else None


def fetch_with_cffi(url, max_retries=MAX_RETRIES):
    proxies = get_proxies()
    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = cffi_requests.get(
                url,
                headers=HEADERS,
                impersonate="chrome131",
                proxies=proxies,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(3)
            else:
                print(f"❌ Failed after {max_retries} attempts for {url}: {e}")
                return None


def get_latest_sitemap_url():
    """Build sitemap URL for the current month; fall back to previous month if it 404s."""
    now = datetime.now()
    for offset in range(0, 3):
        if offset == 0:
            year, month = now.year, now.month
        else:
            total = now.year * 12 + (now.month - 1) - offset
            year, month = divmod(total, 12)
            month += 1

        sitemap_url = f"{SITEMAP_BASE}_{year}-{month:02d}-01.xml"
        print(f"🔎 Trying sitemap: {sitemap_url}")
        html = fetch_with_cffi(sitemap_url)
        if html:
            return sitemap_url, html

    return None, None


def normalize_timestamp(ts):
    """Normalize ISO 8601 timestamps to a consistent format for string comparison.
    Converts trailing 'Z' to '+00:00' so comparisons are consistent."""
    if not ts:
        return ""
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return ts


def parse_sitemap(xml_text):
    """Parse sitemap XML and return list of {url, lastmod} sorted newest-first."""
    soup = BeautifulSoup(xml_text, "xml")
    articles = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        lastmod = url_tag.find("lastmod")
        if loc and lastmod:
            articles.append({
                "url": loc.get_text(strip=True),
                "lastmod": normalize_timestamp(lastmod.get_text(strip=True)),
            })
    articles.sort(key=lambda x: x["lastmod"], reverse=True)
    return articles


def scrape_article(url):
    """Fetch an article page and extract data from JSON-LD NewsArticle schema."""
    html = fetch_with_cffi(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    ld_data = None
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict) and data.get("@type") == "NewsArticle":
                ld_data = data
                break
        except (json.JSONDecodeError, TypeError):
            continue

    if not ld_data:
        print(f"⚠️  No JSON-LD NewsArticle found for {url}")
        return None

    title = ld_data.get("headline") or ld_data.get("name") or ""
    text = ld_data.get("articleBody") or ""
    date = ld_data.get("datePublished") or ""

    if date:
        try:
            dt = datetime.strptime(date[:19], "%Y-%m-%dT%H:%M:%S")
            date = dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass

    if not title or not text:
        print(f"⚠️  Missing title or body for {url}")
        return None

    return {
        "url": url,
        "date": date,
        "title": title,
        "text": text,
        "lastmod": date,
        "scraper_id": SCRAPER_ID,
    }


def main():
    print("🔍 Fetching Business Live sitemap...")

    sitemap_url, xml_text = get_latest_sitemap_url()
    if not sitemap_url:
        print("⛔ Could not fetch any sitemap.")
        return

    article_entries = parse_sitemap(xml_text)
    print(f"📄 Found {len(article_entries)} articles in sitemap.")

    if not article_entries:
        print("⛔ No articles found in sitemap.")
        return

    newest_timestamp = article_entries[0]["lastmod"]

    # Collect saved timestamps for every company
    company_timestamps = {
        config["company_id"]: get_latest_timestamp(SCRAPER_ID, config["company_id"])
        for config in COMPANY_CONFIGS
    }

    # Determine which URLs need scraping across all active companies
    urls_to_scrape = set()
    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        if not is_subscription_active(SCRAPER_ID, company_id):
            continue
        ts = normalize_timestamp(company_timestamps[company_id])
        if ts is not None:
            for entry in article_entries:
                if entry["lastmod"] > ts:
                    urls_to_scrape.add(entry["url"])

    # Scrape each unique article once with threading
    scraped_cache = {}
    if urls_to_scrape:
        entries_to_scrape = [e for e in article_entries if e["url"] in urls_to_scrape]
        print(f"🔎 Scraping {len(entries_to_scrape)} unique article(s) with {MAX_WORKERS} threads...")

        def scrape_one(entry):
            print("Scraping:", entry["url"])
            result = scrape_article(entry["url"])
            return entry["url"], result

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(scrape_one, e): e for e in entries_to_scrape}
            for future in as_completed(futures):
                url, result = future.result()
                if result:
                    scraped_cache[url] = result

    # Insert scraped articles for each active company
    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        label = config["label"]
        ts = normalize_timestamp(company_timestamps[company_id])

        if not is_subscription_active(SCRAPER_ID, company_id):
            print(f"\n⏭️  Skipping {label} — subscription is inactive")
            continue

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        if ts is None:
            print("🟢 First run detected — NOT scraping any articles.")
            print("Saving latest timestamp:", newest_timestamp)
            update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        print("Previously saved timestamp:", ts)

        new_entries = [e for e in article_entries if e["lastmod"] > ts]

        if not new_entries:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_entries)} new article(s).")

        company_articles = []
        for entry in new_entries:
            cached = scraped_cache.get(entry["url"])
            if cached:
                article = dict(cached)
                article["company_id"] = company_id
                company_articles.append(article)

        if company_articles:
            inserted_count = insert_articles(company_articles)
            print(f"✅ Inserted {inserted_count} articles for {label}")

        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        print(f"🕒 New latest timestamp saved for {label}: {newest_timestamp}")


if __name__ == "__main__":
    main()
