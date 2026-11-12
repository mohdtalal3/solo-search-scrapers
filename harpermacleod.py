import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

from curl_cffi import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://www.harpermacleod.co.uk"
SOURCE_NAME = "HARPER_MACLEOD"
SCRAPER_ID = 94
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

# News and insights are published as WordPress pages, so the article URLs
# come from the newest page sitemap (auto-discovered from the sitemap index,
# so future page-sitemap4.xml, page-sitemap5.xml, ... are picked up automatically).
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap_index.xml"
NEWS_PATHS = ("/news/", "/insights/")
LISTING_PATHS = ("/news/", "/insights/")


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


def is_article_url(url):
    """News/insights article pages, excluding the listing pages themselves."""
    path = url.replace(BASE_URL, "")
    if not path.startswith(NEWS_PATHS):
        return False
    return path not in LISTING_PATHS


def get_latest_page_sitemap():
    """Fetch the sitemap index and return the highest-numbered page-sitemap URL."""
    xml_content = fetch_url(SITEMAP_INDEX_URL)
    if not xml_content:
        raise RuntimeError("Failed to fetch sitemap index")

    root = ET.fromstring(xml_content)
    links = []
    for sitemap in root.findall("{*}sitemap"):
        loc = sitemap.find("{*}loc")
        if loc is not None and "page-sitemap" in loc.text:
            links.append(loc.text.strip())

    if not links:
        raise RuntimeError("No page-sitemap links found in sitemap index")

    # Extract trailing number from URLs like "page-sitemap3.xml" (no number = 1)
    def get_sitemap_number(url):
        try:
            num = url.split("page-sitemap")[1].split(".xml")[0]
            return int(num) if num else 1
        except Exception:
            return 0

    links.sort(key=get_sitemap_number)
    return links[-1]  # The one with the highest number holds the newest pages


def get_articles_from_sitemap(sitemap_url):
    """Fetch a page sitemap and return news/insights entries with url + lastmod."""
    xml_content = fetch_url(sitemap_url)
    if not xml_content:
        print(f"⚠️  Could not fetch sitemap: {sitemap_url}")
        return []

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"❌ XML parse error for {sitemap_url}: {e}")
        return []

    entries = []
    for url_el in root.findall("{*}url"):
        loc_el = url_el.find("{*}loc")
        lastmod_el = url_el.find("{*}lastmod")
        if loc_el is None:
            continue
        url = loc_el.text.strip()
        if not is_article_url(url):
            continue
        lastmod = lastmod_el.text.strip() if lastmod_el is not None else ""
        entries.append({"url": url, "lastmod": lastmod})

    return entries


def scrape_article(url, lastmod):
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title — h1.pageTitle
    title_tag = soup.select_one("h1.pageTitle")
    title = title_tag.get_text(strip=True) if title_tag else ""
    if not title:
        print(f"⚠️  No title found, skipping: {url}")
        return None

    # Body — div.contentWrap
    content = soup.select_one("div.contentWrap")
    if not content:
        return None

    for tag in content.select("script, style, iframe, form, .sharedaddy, .wp-block-jetpack"):
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

    print("🔍 Fetching Harper Macleod sitemap index...")
    latest_sitemap = get_latest_page_sitemap()
    print("Using sitemap:", latest_sitemap)

    article_entries = get_articles_from_sitemap(latest_sitemap)

    if not article_entries:
        print("⛔ No news/insights entries found in sitemaps.")
        return

    # Sort newest first by lastmod
    article_entries.sort(key=lambda x: x["lastmod"], reverse=True)
    newest_timestamp = article_entries[0]["lastmod"]
    print(f"📋 Found {len(article_entries)} news/insights URLs in sitemaps.")

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
            for entry in article_entries:
                if entry["lastmod"] > ts:
                    urls_to_scrape.add(entry["url"])

    # Scrape each unique article once (threaded, max 5 workers)
    scraped_cache = {}
    if urls_to_scrape:
        lastmod_by_url = {e["url"]: e["lastmod"] for e in article_entries}
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
            print("Saving latest timestamp:", newest_timestamp)
            update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        # ----------------------------
        # SUBSEQUENT RUNS — save new
        # ----------------------------
        print("Previously saved timestamp:", saved_timestamp)

        new_entries = [e for e in article_entries if e["lastmod"] > saved_timestamp]

        if not new_entries:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_entries)} new articles.")

        company_articles = [
            dict(cached, company_id=company_id)
            for e in new_entries
            if (cached := scraped_cache.get(e["url"]))
        ]

        if company_articles:
            inserted_count = insert_articles(company_articles)
            print(f"✅ Inserted {inserted_count} articles for {label}")

        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
