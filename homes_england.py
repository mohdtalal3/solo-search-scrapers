import json
import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles

load_dotenv()

SOURCE_NAME = "HOMES_ENGLAND"
SCRAPER_ID = 22
COMPANY_ID = os.getenv("PLEA_COMPANY_ID")

BASE_URL = "https://www.gov.uk"
SEARCH_ENDPOINT = "https://www.gov.uk/search/all"
SEARCH_PARAMS = {
    "organisations[]": "homes-england",
    "order": "updated-newest",
    "parent": "homes-england",
}
MAX_PAGES = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def normalize_date(raw: str) -> str:
    """Parse an ISO 8601 date string and return it as YYYY-MM-DDTHH:MM:SS."""
    if not raw:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw[:25], fmt[:len(fmt)])
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return raw


def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def fetch_listing():
    """Fetch up to MAX_PAGES of search results and return list of (full_url, title) tuples."""
    items = []
    for page in range(1, MAX_PAGES + 1):
        try:
            time.sleep(1)
            params = {**SEARCH_PARAMS, "page": page}
            resp = requests.get(SEARCH_ENDPOINT, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            all_lis = soup.select("ul.gem-c-document-list li.gem-c-document-list__item")
            if not all_lis:
                print(f"  ℹ️  No results on page {page}, stopping pagination.")
                break

            page_items = []
            for li in all_lis:
                a = li.select_one("div.gem-c-document-list__item-title a")
                if not a:
                    continue
                href = a.get("href", "")
                if not href:
                    continue
                full_url = BASE_URL + href if href.startswith("/") else href
                if "/publications/" in full_url:
                    continue
                title = a.get_text(strip=True)
                # Extract updated date from the listing item
                time_el = li.select_one("ul.gem-c-document-list__item-metadata time")
                date = normalize_date(time_el.get("datetime", "")) if time_el else ""
                page_items.append((full_url, title, date))

            print(f"  📄 Page {page}: {len(page_items)} article(s) found.")
            items.extend(page_items)

        except Exception as e:
            print(f"❌ Failed to fetch listing page {page}: {e}")
            break

    return items


def scrape_article(url, fallback_title, listing_date="", max_retries=3):
    """Scrape title and body from a GOV.UK article. Uses listing_date as the date."""
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Try JSON-LD first for title and body
            ld = None
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if data.get("@type") in ("NewsArticle", "Article"):
                        ld = data
                        break
                except json.JSONDecodeError:
                    continue

            if ld:
                title = ld.get("name") or ld.get("headline") or fallback_title
                raw_body = ld.get("articleBody", "")
                body = BeautifulSoup(raw_body, "html.parser").get_text(" ", strip=True)
            else:
                h1 = soup.select_one("h1")
                title = h1.get_text(" ", strip=True) if h1 else fallback_title
                body_div = soup.select_one("div.govspeak")
                body = body_div.get_text(" ", strip=True) if body_div else ""

            return title, listing_date, body

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"  ❌ Failed to scrape {url}: {e}")
                return None, None, None


def main():
    print("🔍 Fetching Homes England articles...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")

    seen_slugs = {url_slug(u) for u in known_urls}

    items = fetch_listing()
    print(f"  🔍 Listing returned {len(items)} articles.")

    new_items = []
    for full_url, title, date in items:
        slug = url_slug(full_url)
        if full_url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_items.append((full_url, title, date))
        seen_slugs.add(slug)

    if not new_items:
        print("\n⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

    articles = []
    for full_url, fallback_title, listing_date in new_items:
        print(f"  Scraping: {full_url}")
        title, date, body = scrape_article(full_url, fallback_title, listing_date=listing_date)
        if title is None:
            continue
        articles.append({
            "url": full_url,
            "date": date,
            "title": title,
            "text": body,
            "company_id": COMPANY_ID,
            "scraper_id": SCRAPER_ID,
        })
        print(f"  ✅ {title[:60]}...")

    if not articles:
        print("\n⛔ No articles scraped successfully.")
        return

    print(f"\n🆕 Found {len(articles)} new article(s) in total.")
    inserted_count = insert_articles(articles)
    print(f"✅ Inserted {inserted_count} articles into database")


if __name__ == "__main__":
    main()
