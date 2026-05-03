import os
import time
from datetime import datetime

from curl_cffi import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://www.fca.org.uk"
SEARCH_URL = (
    "https://www.fca.org.uk/news/search-results"
    "?n_search_term="
    "&category=news%20stories%2Cpress%20releases%2Cstatements"
    "&topic_tags=f.Tag%7CfcaSearchTag%3DConsumer%20Duty"
    "%2Cf.Tag%7CfcaSearchTag%3DFinancial%20crime"
    "%2Cf.Tag%7CfcaSearchTag%3DInsurance"
    "&sort_by=dmetaZ"
)
SOURCE_NAME = "FCA_NEWS"
SCRAPER_ID = 35
COMPANY_ID = os.getenv("MIDDLESEX_PARTNERSHIP_COMPANY_ID")
MAX_PAGES = 1
RESULTS_PER_PAGE = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}


def get_proxies():
    proxy = os.getenv("SCRAPER_PROXY")
    return {"http": proxy, "https": proxy} if proxy else None


def fetch(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(
                url,
                headers=HEADERS,
                proxies=get_proxies(),
                impersonate="chrome131",
                timeout=30,
            )
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


def parse_listing_date(raw: str) -> str:
    """Parse 'Published: DD/MM/YYYY' → 'YYYY-MM-DDTHH:MM:SS'."""
    try:
        clean = raw.replace("Published:", "").strip()
        return datetime.strptime(clean, "%d/%m/%Y").strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return ""


def url_slug(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1]


def parse_listing(html: str) -> list:
    """Return list of (url, title, date_str) from a search results page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for li in soup.select("ol.search-list li.search-item"):
        a = li.select_one("a.search-item__clickthrough")
        if not a:
            continue
        href = a.get("href", "")
        url = href if href.startswith("http") else BASE_URL + href
        title = a.get_text(" ", strip=True)

        date_p = li.select_one("p.meta-item.published-date")
        date_str = parse_listing_date(date_p.get_text(strip=True)) if date_p else ""

        results.append((url, title, date_str))
    return results


def scrape_article(url: str, fallback_title: str = "", fallback_date: str = "") -> dict | None:
    html = fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.select_one("h1.page-header")
    title = h1.get_text(" ", strip=True) if h1 else fallback_title

    # Date — prefer <time datetime="..."> in article header
    date = fallback_date
    time_el = soup.select_one("div.article-meta time")
    if time_el and time_el.get("datetime"):
        date = time_el["datetime"][:19]

    # Body — all copy-block sections inside the article element
    article = soup.select_one("article.article-header")
    body_parts = []
    if article:
        for block in article.select(".copy-block .container"):
            for tag in block.select("script, style, form, nav"):
                tag.decompose()
            text = block.get_text(" ", strip=True)
            if text:
                body_parts.append(text)
    text = " ".join(body_parts)

    return {
        "url": url,
        "title": title,
        "text": text,
        "date": date,
        "scraper_id": SCRAPER_ID,
        "company_id": COMPANY_ID,
    }


def main():
    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping FCA News — subscription is inactive")
        return

    print("🔍 Scraping FCA News (Consumer Duty / Financial crime / Insurance)...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs in DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    all_items = []
    for page in range(MAX_PAGES):
        start_rank = page * RESULTS_PER_PAGE + 1
        url = SEARCH_URL + f"&start_rank={start_rank}"
        print(f"  📄 Fetching page {page + 1} (start_rank={start_rank})")
        html = fetch(url)
        if not html:
            print(f"  ⚠️  Empty response, stopping pagination.")
            break
        items = parse_listing(html)
        print(f"  📋 Page {page + 1}: {len(items)} article(s) found.")
        if not items:
            print(f"  ℹ️  No items, stopping pagination.")
            break
        all_items.extend(items)

    new_items = []
    for url, title, date in all_items:
        if url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {url_slug(url)}")
            continue
        slug = url_slug(url)
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_items.append((url, title, date))
        seen_slugs.add(slug)

    if not new_items:
        print("⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")
    #new_items=new_items[:2]  # limit to 5 for testing
    articles = []
    for url, title, date in new_items:
        print(f"  Scraping: {url}")
        article = scrape_article(url, title, date)
        if not article:
            continue
        articles.append(article)
        print(f"  ✅ {article['title'][:70]}")

    if not articles:
        print("⛔ No articles scraped successfully.")
        return

    inserted = insert_articles(articles)
    print(f"✅ Inserted {inserted} articles into database.")


if __name__ == "__main__":
    main()
