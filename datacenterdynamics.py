import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

SOURCE_NAME = "DATACENTERDYNAMICS"
SCRAPER_ID = 39
COMPANY_ID = os.getenv("NET_ZERO_SEARCH_COMPANY_ID")

BASE_URL = "https://www.datacenterdynamics.com"
LISTING_URL = "https://www.datacenterdynamics.com/en/news/?term=north-america"
MAX_PAGES = 1

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None


def fetch_html(url: str, max_retries: int = 3) -> str:
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
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"  ❌ Failed after {max_retries} attempts for {url}: {e}")
                return ""


def get_listing_urls(page: int) -> str:
    if page == 1:
        return LISTING_URL
    return f"{LISTING_URL}&page={page}"


def parse_listing_page(html: str) -> list[str]:
    """Extract article URLs from a listing page."""
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.select("article.card a.headline-link"):
        href = a.get("href", "")
        if not href:
            continue
        # Only include /en/news/ articles (skip broadcasts, magazines, etc.)
        if not href.startswith("/en/news/"):
            continue
        full_url = BASE_URL + href.rstrip("/") + "/"
        if full_url not in urls:
            urls.append(full_url)
    return urls


def scrape_article(url: str) -> dict | None:
    html = fetch_html(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title
    title_el = soup.select_one("h1.article-heading")
    title = title_el.get_text(strip=True) if title_el else ""

    # Date — prefer datetime attribute on <time>
    date_el = soup.select_one("time.article-intro__date")
    if date_el and date_el.get("datetime"):
        raw_date = date_el["datetime"]  # e.g. "2026-05-15"
        date = raw_date + "T00:00:00" if "T" not in raw_date else raw_date
    else:
        date = ""

    # Body — div.article-body, strip noise
    body_el = soup.select_one("div.article-body")
    if body_el:
        for tag in body_el.select(
            "script, style, .w-block-channel_subscription, "
            ".w-block-auto_featured_content, .a2a, .ad-unit, "
            ".tag-container, .comments"
        ):
            tag.decompose()
        body = body_el.get_text(" ", strip=True)
    else:
        body = ""

    if not title and not body:
        return None

    return {
        "url": url,
        "title": title,
        "text": body,
        "date": date,
        "scraper_id": SCRAPER_ID,
        "company_id": COMPANY_ID,
    }


def main():
    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping Data Center Dynamics — subscription is inactive")
        return

    print("🔍 Scraping Data Center Dynamics (North America)...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs in DB.")

    # Collect all new article URLs from listing pages
    new_urls: list[str] = []
    for page in range(1, MAX_PAGES + 1):
        listing_url = get_listing_urls(page)
        print(f"  📄 Fetching listing page {page}: {listing_url}")
        html = fetch_html(listing_url)
        if not html:
            print(f"  ⚠️  Empty response for page {page}, stopping.")
            break

        page_urls = parse_listing_page(html)
        print(f"     Found {len(page_urls)} article(s).")

        added = 0
        for url in page_urls:
            if url not in known_urls and url not in new_urls:
                new_urls.append(url)
                added += 1

        if added == 0:
            print("  ✅ All articles on this page already known — stopping pagination.")
            break

    if not new_urls:
        print("⛔ No new articles found.")
        return

    print(f"\n  🆕 {len(new_urls)} new article(s) to scrape.")

    articles = []

    def scrape_one(url: str):
        print(f"  Scraping: {url}")
        result = scrape_article(url)
        if result:
            print(f"  ✅ {result['title'][:70]}")
        return result

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_one, url): url for url in new_urls}
        for future in as_completed(futures):
            result = future.result()
            if result:
                articles.append(result)

    if not articles:
        print("⛔ No articles scraped successfully.")
        return

    inserted = insert_articles(articles)
    print(f"✅ Inserted {inserted} articles into database.")


if __name__ == "__main__":
    main()
