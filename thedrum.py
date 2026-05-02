import json
import os
import time

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles

load_dotenv()

LISTING_URL = "https://www.thedrum.com/latest"
SOURCE_NAME = "THE_DRUM"
SCRAPER_ID = 30
COMPANY_ID = os.getenv("HEADLINERS_COMPANY_ID")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_url(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


def get_listing_urls():
    """Fetch /latest and extract article URLs from the CollectionPage JSON-LD."""
    html = fetch_url(LISTING_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                entries = data
            else:
                entries = [data]

            for entry in entries:
                if entry.get("@type") == "CollectionPage":
                    main_entity = entry.get("mainEntity", {})
                    items = main_entity.get("itemListElement", [])
                    urls = [item["url"] for item in items if "url" in item]
                    print(f"📋 Found {len(urls)} article links on listing page.")
                    return urls
        except (json.JSONDecodeError, KeyError):
            continue

    print("⚠️  Could not find CollectionPage JSON-LD on listing page.")
    return []


def scrape_article(url):
    """Fetch an article page and extract data from NewsArticle JSON-LD."""
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                entries = data
            else:
                entries = [data]

            for entry in entries:
                if entry.get("headline") and entry.get("articleBody"):
                    title = entry.get("headline", "")
                    body = entry.get("articleBody", "")
                    date = entry.get("datePublished", "")[:19]
                    if date:
                        date = date.replace("T", "T")  # already ISO
                    return {
                        "url": url,
                        "title": title,
                        "text": body,
                        "date": date,
                        "lastmod": date,
                        "scraper_id": SCRAPER_ID,
                        "company_id": COMPANY_ID,
                    }
        except (json.JSONDecodeError, KeyError):
            continue

    print(f"⚠️  Could not find NewsArticle JSON-LD for {url}")
    return None


def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def main():
    print("🔍 Fetching The Drum latest page...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    urls = get_listing_urls()
  #  urls=urls[:5]  # limit to 20 for testing
    if not urls:
        print("⛔ No article URLs found.")
        return

    # Deduplicate while preserving order
    seen = set()
    unique_urls = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)
    print(f"🔗 {len(unique_urls)} unique article URL(s) after deduplication.")

    new_urls = []
    for url in unique_urls:
        slug = url_slug(url)
        if url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_urls.append(url)
        seen_slugs.add(slug)

    if not new_urls:
        print("\n⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_urls)} new article(s) to scrape.")

    articles = []
    for url in new_urls:
        print(f"  Scraping: {url}")
        article = scrape_article(url)
        if not article:
            continue
        articles.append(article)
        print(f"  ✅ {article['title'][:60]}...")

    if not articles:
        print("\n⛔ No articles scraped successfully.")
        return

    print(f"\n🆕 Found {len(articles)} new article(s) in total.")
    inserted_count = insert_articles(articles)
    print(f"✅ Inserted {inserted_count} articles into database")


if __name__ == "__main__":
    main()
