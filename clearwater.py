import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://www.clearwatercf.com"
NEWS_LISTING_URL = f"{BASE_URL}/en-gb/news/"
TRANSACTIONS_LISTING_URL = f"{BASE_URL}/en-gb/experience/transactions/"
SOURCE_NAME = "CLEARWATER"
SCRAPER_ID = 71

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


def parse_news_listing(html):
    """Extract article links and titles from the news listing page."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    for card in soup.find_all("a", class_="resources-list__card-inner"):
        href = card.get("href", "")
        if href and href not in seen:
            seen.add(href)
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            title_el = card.find("p", class_="resources-list__card-title")
            title = title_el.get_text(strip=True) if title_el else ""
            items.append((full_url, title))

    return items


def parse_transactions_listing(html):
    """Extract transaction links and titles from the transactions listing page."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    for card in soup.find_all("a", class_="transaction-list-page__resource-inner"):
        href = card.get("href", "")
        if href and href not in seen:
            seen.add(href)
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            title_el = card.find("p", class_="transaction-list-page__resource-title")
            title = title_el.get_text(strip=True) if title_el else ""
            items.append((full_url, title))

    return items


def scrape_article(url, title):
    """Fetch a news article page and extract text and date. Title comes from listing page."""
    html = fetch_with_cffi(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Date from meta tag
    date = ""
    meta_date = soup.find("meta", {"property": "article:published_time"})
    if meta_date:
        date = meta_date.get("content", "")
    if date:
        try:
            dt = datetime.strptime(date[:19], "%Y-%m-%dT%H:%M:%S")
            date = dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass

    # Text from all content sections (image-content and pull-out blocks)
    text_parts = []

    for section in soup.find_all("section", class_=lambda x: x and "module-image-content-block" in x):
        for typ_div in section.find_all("div", class_="image-content__typ"):
            for tag in typ_div.select("script, style, picture, source, img"):
                tag.decompose()
            text = typ_div.get_text(" ", strip=True)
            text = " ".join(text.split())
            if text:
                text_parts.append(text)

    for section in soup.find_all("section", class_=lambda x: x and "module-pull-out-text-block" in x):
        for typ_div in section.find_all("div", class_="pull-out__inner"):
            for tag in typ_div.select("script, style"):
                tag.decompose()
            text = typ_div.get_text(" ", strip=True)
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


def scrape_transaction(url, title):
    """Fetch a transaction page and extract text. Title comes from listing page."""
    html = fetch_with_cffi(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Date from meta tag (transactions may not have one)
    date = ""
    meta_date = soup.find("meta", {"property": "article:published_time"})
    if meta_date:
        date = meta_date.get("content", "")
    if date:
        try:
            dt = datetime.strptime(date[:19], "%Y-%m-%dT%H:%M:%S")
            date = dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass

    text_parts = []

    # Info fields (Transaction, Sell-side, Role, Sector, Region)
    for info_div in soup.find_all("div", class_="transaction-page__info"):
        label_el = info_div.find("span", class_="label")
        p_el = info_div.find("p")
        if label_el and p_el:
            label = label_el.get_text(strip=True)
            value = p_el.get_text(" ", strip=True)
            value = " ".join(value.split())
            if value:
                text_parts.append(f"{label}: {value}")

    # Main content paragraphs
    content_div = soup.find("div", class_="transaction-page__main-content")
    if content_div:
        for tag in content_div.select("script, style, img, picture"):
            tag.decompose()
        for p in content_div.find_all("p"):
            text = p.get_text(" ", strip=True)
            text = " ".join(text.split())
            if text:
                text_parts.append(text)

    # Quotes
    for blockquote in soup.find_all("blockquote", class_="sc-quote"):
        content_p = blockquote.find("p", class_="sc-quote__content")
        author_span = blockquote.find("span", class_="sc-quote__author")
        if content_p:
            quote_text = content_p.get_text(" ", strip=True)
            quote_text = " ".join(quote_text.split())
            author = author_span.get_text(strip=True) if author_span else ""
            if author:
                text_parts.append(f'"{quote_text}" — {author}')
            else:
                text_parts.append(f'"{quote_text}"')

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


def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def main():
    print("🔍 Fetching Clearwater listings...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    # Fetch news listing
    news_items = []
    print("\n📰 Fetching news listing...")
    news_html = fetch_with_cffi(NEWS_LISTING_URL)
    if news_html:
        news_items = parse_news_listing(news_html)
        print(f"  🔗 Found {len(news_items)} news article(s).")
    else:
        print("  ⛔ Failed to fetch news listing.")

    # Fetch transactions listing
    transaction_items = []
    print("\n💼 Fetching transactions listing...")
    tx_html = fetch_with_cffi(TRANSACTIONS_LISTING_URL)
    if tx_html:
        transaction_items = parse_transactions_listing(tx_html)
        print(f"  🔗 Found {len(transaction_items)} transaction(s).")
    else:
        print("  ⛔ Failed to fetch transactions listing.")

    all_listing_items = news_items + transaction_items
    if not all_listing_items:
        print("\n⛔ No items found on any listing page.")
        return

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

    print(f"\n  🆕 {len(new_items)} new item(s) to scrape.")

    # Determine scrape function based on URL path
    def get_scraper(url):
        if "/experience/transactions/" in url:
            return scrape_transaction
        return scrape_article

    # Scrape all new items with threading
    all_articles = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_scraper(url), url, title): url
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
