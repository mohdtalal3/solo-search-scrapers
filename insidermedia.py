import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://www.insidermedia.com"
LISTING_URLS = [
    f"{BASE_URL}/news/all/manufacturing",
    f"{BASE_URL}/news/all/deals",
    f"{BASE_URL}/news/all/business",
]
SOURCE_NAME = "INSIDER_MEDIA"
SCRAPER_ID = 72

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

MAX_RETRIES = 3
MAX_WORKERS = 5


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


def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def parse_listing_html(html):
    """Extract article links and titles from a listing page."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    for article in soup.find_all("article", attrs={"itemtype": "http://schema.org/Article"}):
        h2 = article.find("h2", class_="itemTitle")
        if not h2:
            continue
        a = h2.find("a")
        if not a:
            continue
        href = a.get("href", "")
        if href and href not in seen:
            seen.add(href)
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            title = a.get_text(strip=True)
            items.append((full_url, title))
    return items


def scrape_article(url, fallback_title=""):
    """Fetch an article page and extract data directly from HTML."""
    html = fetch_with_cffi(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    article = soup.find("article", class_="article")
    if not article:
        print(f"⚠️  No article tag found for {url}")
        return None

    # Title from h1
    title = ""
    h1 = article.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        title = fallback_title

    # Date from time tag
    date = ""
    time_tag = article.find("time")
    if time_tag:
        date = time_tag.get("datetime", "")
    if date:
        try:
            dt = datetime.strptime(date[:19], "%Y-%m-%dT%H:%M:%S")
            date = dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass

    # Text from all p tags inside article-content div
    text_parts = []
    content_div = article.find("div", id="article-content")
    if content_div:
        for tag in content_div.select("script, style, figure, .ad-container"):
            tag.decompose()
        for p in content_div.find_all("p"):
            text = p.get_text(" ", strip=True)
            text = " ".join(text.split())
            if text:
                text_parts.append(text)

    text = "\n\n".join(text_parts)

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
    print("🔍 Fetching Insider Media listings...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    # Fetch all listing pages
    all_listing_items = []
    for listing_url in LISTING_URLS:
        print(f"\n📰 Fetching: {listing_url}")
        html = fetch_with_cffi(listing_url)
        if html:
            items = parse_listing_html(html)
            print(f"  🔗 Found {len(items)} article(s).")
            all_listing_items.extend(items)
        else:
            print(f"  ⛔ Failed to fetch listing.")

    if not all_listing_items:
        print("\n⛔ No items found on any listing page.")
        return

    print(f"\n📋 Total items across all listings: {len(all_listing_items)}")

    # Filter out already-scraped URLs
    new_items = []
    for url, title in all_listing_items:
        slug = url_slug(url)
        if url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_items.append((url, title))
        seen_slugs.add(slug)

    if not new_items:
        print("\n⛔ No new articles found.")
        return

    print(f"\n  🆕 {len(new_items)} new article(s) to scrape.")

    # Scrape all new articles with threading
    all_articles = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_article, url, title): url
            for url, title in new_items
        }
        for future in as_completed(futures):
            url = futures[future]
            try:
                article = future.result()
                if article:
                    all_articles.append(article)
                    print(f"  ✅ {article['title'][:60]}...")
                else:
                    print(f"  ⛔ Failed to scrape: {url}")
            except Exception as e:
                print(f"  ⛔ Error scraping {url}: {e}")

    if not all_articles:
        print("\n⛔ No articles scraped successfully.")
        return

    print(f"\n🆕 Found {len(all_articles)} new article(s) in total.")

    # Insert for each active company
    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        label = config["label"]

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        if not is_subscription_active(SCRAPER_ID, company_id):
            print(f"⏭️  Skipping {label} — subscription is inactive")
            continue

        company_articles = [dict(a, company_id=company_id) for a in all_articles]
        inserted_count = insert_articles(company_articles)
        print(f"✅ Inserted {inserted_count} articles for {label}")


if __name__ == "__main__":
    main()
