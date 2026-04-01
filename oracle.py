import os
import time
from curl_cffi import requests
from dotenv import load_dotenv
from db import get_recent_article_urls, insert_articles

load_dotenv()

API_URL = "https://search-api.oracle.com/api/v1/search/news"
SOURCE_NAME = "ORACLE"
SCRAPER_ID = 20
COMPANY_ID = os.getenv("ERP_RECRUIT_COMPANY_ID")

# All regions to scrape — each is fetched independently
REGIONS = [
    {"country": "Netherlands", "locale": "nl"},
    {"country": "Belgium",     "locale": "be"},
   # {"country": "Luxembourg",  "locale": "lu"},
    {"country": "United Kingdom", "locale": "uk"},
]

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://www.oracle.com",
    "accept-language": "en-US,en;q=0.9",
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}

PAGE_SIZE = 30


def fetch_page(country, locale, offset, max_retries=3):
    payload = {
        "q": "",
        "locale": locale,
        "country": country,
        "size": PAGE_SIZE,
        "offset": offset,
    }
    hdrs = {**HEADERS, "referer": f"https://www.oracle.com/{locale}/news/"}
    for attempt in range(max_retries):
        try:
            time.sleep(2)
            response = requests.post(
                API_URL,
                json=payload,
                headers=hdrs,
                impersonate="chrome107",
                timeout=30,
            )
            #print(response.text)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


def to_iso(display_date):
    """Convert '2026-03-24 08:00:00' → '2026-03-24T08:00:00'"""
    return display_date.replace(" ", "T")


def url_slug(url):
    """Extract the final path segment from a URL, e.g.:
    'https://www.oracle.com/nl/news/announcement/oracle-introduces-fusion-agentic-applications-2026-03-24/'
    → 'oracle-introduces-fusion-agentic-applications-2026-03-24'
    This lets us deduplicate the same article across different regional URLs.
    """
    return url.rstrip("/").rsplit("/", 1)[-1]


def fetch_region(country, locale, known_urls, seen_slugs):
    """Fetch one page for a region and return articles not already seen."""
    articles = []
    print(f"\n🌍 Region: {country} ({locale})")

    data = fetch_page(country, locale, offset=0)
    if not data or not data.get("results"):
        print(f"  ⛔ No results for {country}.")
        return articles

    for item in data["results"]:
        src = item.get("_source", {})
        url = src.get("display_url", "").strip()
        title = src.get("title", "").strip()
        display_date = src.get("display_date", "")

        if not url or not title or not display_date:
            continue

        slug = url_slug(url)

        # Skip if full URL already in DB or slug already seen this run
        if url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {title[:60]}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug across regions): {title[:60]}")
            continue

        timestamp = to_iso(display_date)
        body = (src.get("body") or "").strip()

        articles.append({
            "url": url,
            "date": timestamp,
            "title": title,
            "text": body,
            "lastmod": timestamp,
            "company_id": COMPANY_ID,
            "scraper_id": SCRAPER_ID,
        })
        seen_slugs.add(slug)
        print(f"  Fetched: {title[:60]}...")

    return articles


def main():
    print("🔍 Fetching articles from Oracle News API (all regions)...")

    # Full URLs already in DB — primary dedup against historic data
    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")

    # Slugs seen this run — deduplicates same article under different regional URLs
    seen_slugs = {url_slug(u) for u in known_urls}

    all_articles = []

    for region in REGIONS:
        region_articles = fetch_region(region["country"], region["locale"], known_urls, seen_slugs)
        for a in region_articles:
            known_urls.add(a["url"])
        all_articles.extend(region_articles)

    if not all_articles:
        print("\n⛔ No new articles found across all regions.")
        return

    print(f"\n🆕 Found {len(all_articles)} new article(s) in total.")
    inserted_count = insert_articles(all_articles)
    print(f"✅ Inserted {inserted_count} articles into database")


if __name__ == "__main__":
    main()