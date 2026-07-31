import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from datetime import date as dt_date

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://www.channeldive.com"
SOURCE_NAME = "CHANNEL_DIVE"
SCRAPER_ID = 76

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
    return url.rstrip("/").rsplit("/", 2)[-2] if url.rstrip("/").endswith("/") else url.rstrip("/").rsplit("/", 1)[-1]


def build_sitemap_urls():
    """Build sitemap URLs for current and previous month."""
    today = dt_date.today()
    d = today.replace(day=1)
    month_name = d.strftime("%B").lower()
    return [f"{BASE_URL}/news/archive/{d.year}/{month_name}.xml"]


def parse_sitemap(xml_text):
    """Parse XML sitemap and return list of (url, lastmod) tuples."""
    soup = BeautifulSoup(xml_text, "xml")
    items = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        lastmod = url_tag.find("lastmod")
        if not loc:
            continue
        article_url = loc.get_text(strip=True)
        date_str = lastmod.get_text(strip=True) if lastmod else ""
        date = ""
        if date_str:
            try:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
                date = dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                date = date_str
        items.append((article_url, date))
    return items


def scrape_article_text(html):
    """Extract article body text and date from article page HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Title from h1
    title = ""
    h1 = soup.select_one("h1.display-heading-04")
    if h1:
        title = h1.get_text(" ", strip=True)

    # Date from .published-info
    date = ""
    date_el = soup.select_one(".date .published-info")
    if date_el:
        raw_date = date_el.get_text(" ", strip=True).replace("Published ", "")
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                dt = datetime.strptime(raw_date, fmt)
                date = dt.strftime("%Y-%m-%dT%H:%M:%S")
                break
            except ValueError:
                continue
        if not date:
            date = raw_date

    # Article body
    body = soup.select_one("div.article-body")
    if not body:
        return None, None, None

    # Remove unwanted elements
    for el in body.select(
        ".text-to-speech, "
        ".hybrid-ad-wrapper, "
        ".ed-chart, "
        "script, style, iframe, "
        ".social-share, "
        ".social-icon-list, "
        ".reading-list, "
        ".article-box"
    ):
        el.decompose()

    paragraphs = []
    for el in body.find_all(["p", "h2", "h3", "blockquote", "li"]):
        text = el.get_text(" ", strip=True)
        text = " ".join(text.split())
        if text:
            paragraphs.append(text)

    body_text = "\n\n".join(paragraphs) if paragraphs else None
    return body_text, title, date


def scrape_article(url, fallback_date=""):
    """Fetch an article page and extract body text, title, and date."""
    html = fetch_url(url)
    if not html:
        return None

    text, title, date = scrape_article_text(html)
    if not text:
        print(f"⚠️  Missing body for {url}")
        return None

    if not date and fallback_date:
        date = fallback_date

    return {
        "url": url,
        "date": date,
        "title": title,
        "text": text,
        "lastmod": date,
        "scraper_id": SCRAPER_ID,
    }


def main():
    print("🔍 Fetching Channel Dive sitemaps...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    sitemap_urls = build_sitemap_urls()
    print(f"📅 Sitemap URLs: {sitemap_urls}")

    all_items = []
    seen_urls = set()

    for sitemap_url in sitemap_urls:
        print(f"📄 Fetching: {sitemap_url}")
        xml_text = fetch_url(sitemap_url)
        if not xml_text:
            continue
        items = parse_sitemap(xml_text)
        print(f"  Found {len(items)} article(s).")
        for url, date in items:
            if url not in seen_urls:
                seen_urls.add(url)
                all_items.append((url, date))

    print(f"🔗 Total articles found: {len(all_items)}")

    if not all_items:
        print("⛔ No article URLs found.")
        return

    # Filter out already-scraped URLs
    new_items = []
    for url, date in all_items:
        slug = url_slug(url)
        if url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_items.append((url, date))
        seen_slugs.add(slug)

    if not new_items:
        print("\n⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

    # Scrape all new articles with threading
    all_articles = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_article, url, date): url
            for url, date in new_items
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
