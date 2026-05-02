import os
import time
from datetime import datetime

import requests
from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles

load_dotenv()

LISTING_URL = "https://www.prolificnorth.co.uk/news/?jsf=jet-engine:news-grid&tax=category:54,56,60,41,59"
SOURCE_NAME = "PROLIFIC_NORTH"
SCRAPER_ID = 33
COMPANY_ID = os.getenv("HEADLINERS_COMPANY_ID")
MAX_PAGES = 1

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def url_slug(url):
    return url.rstrip("/").split("/")[-1]


# ----------------------------------------------------------
# curl_cffi fetch (for listing — JS-rendered JetSmartFilters)
# ----------------------------------------------------------
def fetch_with_cffi(url, max_retries=3):
    proxy = os.getenv("SCRAPER_PROXY")
    proxies = {"http": proxy, "https": proxy} if proxy else None

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
            print(f"⚠️  cffi error (attempt {attempt + 1}): {e}")
            time.sleep(3)

    print(f"❌ Failed after {max_retries} attempts for: {url}")
    return None


# ----------------------------------------------------------
# Plain requests fetch (for article pages — server-rendered)
# ----------------------------------------------------------
def fetch_url(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                print(f"❌ Failed to fetch {url}: {e}")
                return None


def parse_listing_html(html):
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    seen = set()

    # The listing grid has id="news-grid" on first page; fallback to all items
    grid = soup.find("div", id="news-grid")
    items = grid.find_all("div", class_="jet-listing-grid__item") if grid else \
            soup.find_all("div", class_="jet-listing-grid__item")

    for item in items:
        h2 = item.find("h2", class_="elementor-heading-title")
        if not h2:
            continue
        a = h2.find("a", href=True)
        if a and a["href"] not in seen:
            seen.add(a["href"])
            urls.append(a["href"])

    return urls


def parse_date(date_str):
    """Parse 'May 1, 2026' → '2026-05-01T00:00:00'"""
    try:
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def scrape_article(url):
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.find("h1", class_="elementor-heading-title")
    if not h1:
        return None
    title = h1.get_text(strip=True)

    # Date from <li itemprop="datePublished"><span...><time>May 1, 2026</time>
    date = None
    date_li = soup.find("li", attrs={"itemprop": "datePublished"})
    if date_li:
        time_el = date_li.find("time")
        if time_el:
            date = parse_date(time_el.get_text(strip=True))

    # Body — only the theme-post-content widget; excludes Related News grid
    text = ""
    content_widget = soup.find("div", attrs={"data-widget_type": "theme-post-content.default"})
    if content_widget:
        container = content_widget.find("div", class_="elementor-widget-container")
        if container:
            for tag in container.select("script, style, iframe, .bsf-rt-reading-time"):
                tag.decompose()
            text = container.get_text(" ", strip=True)
            text = " ".join(text.split())

    if not title or not text:
        return None

    return {
        "url": url,
        "date": date or "",
        "title": title,
        "text": text,
        "lastmod": date or "",
        "company_id": COMPANY_ID,
        "scraper_id": SCRAPER_ID,
    }


def main():
    print("🔍 Fetching Prolific North listing pages...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    all_urls = []

    for page_num in range(1, MAX_PAGES + 1):
        url = LISTING_URL if page_num == 1 else f"{LISTING_URL}&paged={page_num}"
        print(f"📄 Fetching listing page {page_num}...")

        html = fetch_with_cffi(url)
        if not html:
            print(f"⛔ Empty response for page {page_num}, stopping.")
            break

        urls = parse_listing_html(html)
        if not urls:
            print(f"⛔ No articles found on page {page_num}, stopping.")
            break

        all_urls.extend(urls)
        print(f"  Found {len(urls)} article(s) on page {page_num}")

    if not all_urls:
        print("⛔ No article URLs found.")
        return

    # Deduplicate while preserving order
    seen = set()
    unique_urls = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)
    print(f"🔗 {len(unique_urls)} unique article URL(s) after deduplication.")

    new_urls = []
    for u in unique_urls:
        slug = url_slug(u)
        if u in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_urls.append(u)
        seen_slugs.add(slug)

    if not new_urls:
        print("\n⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_urls)} new article(s) to scrape.")
    #new_urls = new_urls[:3]  # limit for testing
    articles = []
    for u in new_urls:
        print(f"  Scraping: {u}")
        article = scrape_article(u)
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
