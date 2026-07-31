import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://rcpmag.com"
LISTING_URL = f"{BASE_URL}/articles/list/news.aspx"
SOURCE_NAME = "RCP_MAG"
SCRAPER_ID = 74

COMPANY_CONFIGS = [
    {
        "label": "Intune Talent",
        "company_id": os.getenv("INTUNE_TALENT_COMPANY_ID"),
    },
]

MAX_RETRIES = 3
MAX_WORKERS = 5

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
}

_proxy = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": _proxy, "https": _proxy} if _proxy else None


def fetch_url(url, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30, proxies=PROXIES)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for {url}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts for {url}: {str(e)}")
                return None


def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def parse_listing_html(html):
    """Extract article links, titles, and dates from the listing page."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen_urls = set()

    article_div = soup.find("div", id="article")
    if not article_div:
        print("⚠️  No div#article found on listing page")
        return items

    for card in article_div.find_all("div", recursive=False):
        title_el = card.select_one("h3.title")
        if not title_el:
            continue
        link_el = title_el.find("a", href=True)
        if not link_el:
            continue
        title = link_el.get_text(" ", strip=True)
        article_url = urljoin(BASE_URL, link_el["href"])
        if article_url in seen_urls:
            continue
        seen_urls.add(article_url)

        date = ""
        date_el = card.select_one("li.date")
        if date_el:
            date = date_el.get_text(" ", strip=True)

        items.append((article_url, title, date))

    return items


def scrape_article_text(html):
    """Extract article body text from article page HTML."""
    soup = BeautifulSoup(html, "html.parser")

    article_div = soup.find("div", id="article")
    if not article_div:
        return None

    for el in article_div.select(
        ".socialshare__wrapper, "
        ".share__list, "
        ".sidebar, "
        "div.ad, "
        "div[id^='div-gpt-ad'], "
        "iframe, "
        "script, style, "
        ".kicker, "
        ".byline, "
        "figure"
    ):
        el.decompose()

    h3_title = article_div.select_one("h3.title")
    if h3_title:
        h3_title.decompose()

    paragraphs = []
    for el in article_div.find_all(["p", "h2", "h3", "blockquote"]):
        text = el.get_text(" ", strip=True)
        text = " ".join(text.split())
        if text:
            paragraphs.append(text)

    return "\n\n".join(paragraphs) if paragraphs else None


def scrape_article(url, fallback_title="", fallback_date=""):
    """Fetch an article page and extract body text.
    Title and date come from the listing page."""
    html = fetch_url(url)
    if not html:
        return None

    text = scrape_article_text(html)
    if not text:
        print(f"⚠️  Missing body for {url}")
        return None

    date = ""
    if fallback_date:
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(fallback_date, fmt)
                date = dt.strftime("%Y-%m-%dT%H:%M:%S")
                break
            except ValueError:
                continue
        if not date:
            date = fallback_date

    return {
        "url": url,
        "date": date,
        "title": fallback_title,
        "text": text,
        "lastmod": date,
        "scraper_id": SCRAPER_ID,
    }


def main():
    print("🔍 Fetching RCP Magazine news listing...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    html = fetch_url(LISTING_URL)
    if not html:
        print("⛔ Failed to fetch listing page.")
        return

    all_items = parse_listing_html(html)
    print(f"🔗 Found {len(all_items)} article(s) on listing page.")

    if not all_items:
        print("⛔ No article URLs found.")
        return

    # Filter out already-scraped URLs
    new_items = []
    for url, title, date in all_items:
        slug = url_slug(url)
        if url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_items.append((url, title, date))
        seen_slugs.add(slug)

    if not new_items:
        print("\n⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

    # Scrape all new articles with threading
    all_articles = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_article, url, title, date): url
            for url, title, date in new_items
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
