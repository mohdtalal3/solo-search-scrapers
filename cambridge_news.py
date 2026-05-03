import os
import time
from datetime import datetime

import requests
from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles


load_dotenv()

SOURCE_NAME = "CAMBRIDGE_NEWS"
SCRAPER_ID = 28
COMPANY_ID = os.getenv("PLEA_COMPANY_ID")

API_URL = "https://api.mantis-intelligence.com/reach/search"

KEYWORDS = [
    "planning permission",
    "planning approval",
    "planning approved",
    "development approved",
    "homes approved",
    "dwellings",
    "housing development",
    "new homes",
    "residential scheme",
    "land acquisition",
    "site secured",
    "developer",
    "housing association",
    "Cambridge",
    "South Cambridgeshire",
    "Huntingdon",
    "Ely",
    "Peterborough",
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




def get_proxies():
    proxy = os.getenv("SCRAPER_PROXY")
    return {"http": proxy, "https": proxy} if proxy else None


def normalize_date(raw: str) -> str:
    """Convert '2026-04-15T11:02:11.000Z' to '2026-04-15T11:02:11'."""
    if not raw:
        return ""
    return raw[:19]


def fetch_keyword_results(keyword: str) -> tuple[int, list[dict]]:
    """Fetch first page (limit=30) of results for a keyword from the Mantis API."""
    params = {
        "search_text_all": keyword,
        "search_text": "",
        "search_text_none": "",
        "mantis_categories": "",
        "tags": "",
        "domains": "cambridge-news",
        "excluded_domains": "",
        "author": "",
        "start": 0,
        "limit": 20,
        "sort": "date",
        "indexAlias": "12-months",
    }
    try:
        resp = requests.get(
            API_URL,
            params=params,
            headers={**HEADERS, "Accept": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        total = data.get("totalNumberofArticlesMatched", 0)
        articles = data.get("articleData", [])
        return total, articles
    except Exception as e:
        print(f"❌ Failed to fetch keyword '{keyword}': {e}")
        return 0, []


def scrape_article(url: str, max_retries: int = 3):
    """Scrape body text from a cambridge-news.co.uk article page."""
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = cffi_requests.get(
                url,
                headers=HEADERS,
                proxies=get_proxies(),
                impersonate="chrome131",
                timeout=30,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Selecting only content paragraphs naturally excludes all ad boxes
            paragraphs = soup.find_all(
                "p",
                class_=lambda c: c and "Paragraph_paragraph-text" in c,
            )
            if paragraphs:
                return " ".join(p.get_text(" ", strip=True) for p in paragraphs)

            # Fallback: article body element
            article = soup.find("article", id="article-body")
            if article:
                for tag in article.find_all(
                    ["script", "style", "iframe", "mantis-ui-widget"]
                ):
                    tag.decompose()
                return article.get_text(" ", strip=True)

            return ""

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"  ❌ Failed to scrape {url}: {e}")
                return None


def main():
    print("🔍 Fetching Cambridge News articles...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")

    seen_urls = set(known_urls)
    # url -> {title, date} — ordered dict preserves insertion order (Python 3.7+)
    new_articles_meta: dict[str, dict] = {}

    for keyword in KEYWORDS:
        time.sleep(0.5)
        total, items = fetch_keyword_results(keyword)
        added = 0
        for item in items:
            raw_url = item.get("url", "")
            if not raw_url:
                continue
            full_url = (
                f"https://{raw_url}" if not raw_url.startswith("http") else raw_url
            )
            if full_url not in seen_urls:
                seen_urls.add(full_url)
                new_articles_meta[full_url] = {
                    "title": item.get("title", ""),
                    "date": normalize_date(item.get("publishedDate", "")),
                }
                added += 1
        print(f"  🔑 '{keyword}': {total} total match(es), {added} new link(s).")

    if not new_articles_meta:
        print("\n⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_articles_meta)} new article(s) to scrape.")

    articles = []
    for full_url, meta in new_articles_meta.items():
        print(f"  Scraping: {full_url}")
        body = scrape_article(full_url)
        if body is None:
            continue
        articles.append({
            "url": full_url,
            "date": meta["date"],
            "title": meta["title"],
            "text": body,
            "company_id": COMPANY_ID,
            "scraper_id": SCRAPER_ID,
        })
        print(f"  ✅ {meta['title'][:60]}...")

    if not articles:
        print("\n⛔ No articles scraped successfully.")
        return

    print(f"\n🆕 Found {len(articles)} new article(s) in total.")
    inserted_count = insert_articles(articles)
    print(f"✅ Inserted {inserted_count} articles into database")


if __name__ == "__main__":
    main()
