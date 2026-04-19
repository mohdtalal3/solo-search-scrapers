import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles

load_dotenv()

SOURCE_NAME = "EASTCAMBS"
SCRAPER_ID = 26
COMPANY_ID = os.getenv("PLEA_COMPANY_ID")

BASE_URL = "https://eastcambs.gov.uk"
SEARCH_URL = f"{BASE_URL}/search"
MAX_PAGES = 2

KEYWORDS = [
    "landscaping",
    "landscape condition",
    "planting",
    "biodiversity net gain",
    "BNG",
    "ecological",
    "dwellings",
    "residential",
    "housing",
    "mixed use",
    "commercial campus",
    "Cambridge",
    "Cambridgeshire",
    "Huntingdon",
    "Peterborough",
    "Ely",
    "Newmarket",
]

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
    """Parse a date string and return it as YYYY-MM-DDTHH:MM:SS."""
    if not raw:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw[:25], fmt[: len(fmt)])
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return raw


def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def fetch_keyword_links(keyword: str) -> list[str]:
    """Fetch all article links for a given keyword across up to MAX_PAGES pages."""
    links = []
    for page in range(MAX_PAGES):
        try:
            time.sleep(1)
            params = {"s": keyword, "type": 1, "sort_by": "changed_1"}
            if page > 0:
                params["page"] = page
            resp = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            articles = soup.select("ol.search-results li article h2 a")
            if not articles:
                break

            for a in articles:
                href = a.get("href", "")
                if not href:
                    continue
                full_url = BASE_URL + href if href.startswith("/") else href
                links.append(full_url)

        except Exception as e:
            print(f"❌ Failed to fetch keyword '{keyword}' page {page + 1}: {e}")
            break

    return links


def fetch_listing() -> list[str]:
    """Fetch unique article links across all keywords."""
    seen = set()
    all_links = []
    for keyword in KEYWORDS:
        links = fetch_keyword_links(keyword)
        print(f"  🔑 '{keyword}': {len(links)} link(s) found.")
        for link in links:
            if link not in seen:
                seen.add(link)
                all_links.append(link)
    return all_links


def scrape_article(url, max_retries=3):
    """Scrape title, date, and body from an eastcambs.gov.uk article."""
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Title
            h1 = soup.select_one("h1.news-article__title span") or soup.select_one("h1")
            title = h1.get_text(strip=True) if h1 else url

            # Date from <time datetime="...">
            time_el = soup.select_one("time.news-article__metadata-item--date")
            date = normalize_date(time_el.get("datetime", "")) if time_el else ""

            # Body
            body_div = (
                soup.select_one("div.news-article__content")
                or soup.select_one("div.field--name-body")
            )
            body = body_div.get_text(" ", strip=True) if body_div else ""

            return title, date, body

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"  ❌ Failed to scrape {url}: {e}")
                return None, None, None


def main():
    print("🔍 Fetching East Cambridgeshire articles...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    all_links = fetch_listing()
    print(f"  🔍 Total unique links found: {len(all_links)}")

    new_links = []
    for full_url in all_links:
        slug = url_slug(full_url)
        if full_url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_links.append(full_url)
        seen_slugs.add(slug)

    if not new_links:
        print("\n⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_links)} new article(s) to scrape.")

    articles = []
    for full_url in new_links:
        print(f"  Scraping: {full_url}")
        title, date, body = scrape_article(full_url)
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
