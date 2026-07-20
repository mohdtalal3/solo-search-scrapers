import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://www.theengineer.co.uk"
LISTING_URL = f"{BASE_URL}/news"
SOURCE_NAME = "THE_ENGINEER"
SCRAPER_ID = 73

COMPANY_CONFIGS = [
    {
        "label": "1492 Search",
        "company_id": os.getenv("1492_SEARCH_COMPANY_ID"),
    },
]

SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
SCRAPPEY_PROXY_COUNTRY = "UnitedKingdom"

MAX_RETRIES = 3
MAX_WORKERS = 5


def fetch_with_scrappey(url, max_retries=MAX_RETRIES):
    scrappey_api_key = os.getenv("SCRAPPEY_API_KEY")
    if not scrappey_api_key:
        raise RuntimeError("Please set SCRAPPEY_API_KEY in your environment")

    payload = {
        "cmd": "request.get",
        #"requestType": "request",
        "url": url,
        "proxyCountry": SCRAPPEY_PROXY_COUNTRY,
        "premiumProxy": True,
    }

    for attempt in range(max_retries):
        try:
            time.sleep(2)
            response = requests.post(
                f"{SCRAPPEY_API_URL}?key={scrappey_api_key}",
                json=payload,
                timeout=90,
            )
            response.raise_for_status()

            scrappey_response = response.json()
            solution = scrappey_response.get("solution", {})
            status_code = solution.get("statusCode")

            if status_code and status_code != 200:
                print(f"❌ Scrappey returned status {status_code} for {url}")
                return None

            if scrappey_response.get("data") == "error" or not solution.get("verified", False):
                error_message = scrappey_response.get("error", "Unknown Scrappey error")
                raise RuntimeError(error_message)

            return solution.get("response") or ""

        except (requests.RequestException, RuntimeError) as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for {url}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts for {url}: {str(e)}")
                return None


def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def parse_listing_html(html):
    """Extract article links, titles, and dates from the news listing page."""
    from urllib.parse import urljoin

    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen_urls = set()

    for card in soup.select('div[itemscope][itemtype="http://schema.org/Article"]'):
        title_el = card.select_one('h3[itemprop="name"]')
        if not title_el:
            continue
        link_el = title_el.find_parent("a", href=True)
        if not link_el:
            continue
        title = title_el.get_text(" ", strip=True)
        article_url = urljoin(BASE_URL, link_el["href"])
        if article_url in seen_urls:
            continue
        seen_urls.add(article_url)

        date = ""
        date_el = card.select_one('[itemprop="datePublished"]')
        if date_el:
            date = date_el.get("datetime", "").strip() or date_el.get_text(" ", strip=True)

        items.append((article_url, title, date))

    return items


def scrape_article_text(html):
    """Extract article body text from article page HTML."""
    soup = BeautifulSoup(html, "html.parser")

    article_body = soup.select_one("div.articleBody")
    if not article_body:
        return None

    for el in article_body.select(
        ".breakout-box, "
        "#blueconic-quarter-article, "
        "#blueconic-mid-article, "
        "#blueconic-threequarter-article, "
        "script, style"
    ):
        el.decompose()

    paragraphs = []
    for el in article_body.select("p, h2, h3, blockquote"):
        text = el.get_text(" ", strip=True)
        if text:
            paragraphs.append(text)

    return "\n\n".join(paragraphs)


def scrape_article(url, fallback_title="", fallback_date=""):
    """Fetch an article page and extract only the body text.
    Title and date come from the listing page."""
    html = fetch_with_scrappey(url)
    if not html:
        return None

    text = scrape_article_text(html)
    if not text:
        print(f"⚠️  Missing body for {url}")
        return None

    # Parse date from listing page fallback
    date = ""
    if fallback_date:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%d %b %Y"):
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
    print("🔍 Fetching The Engineer news listing...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    html = fetch_with_scrappey(LISTING_URL)
    if not html:
        print("⛔ Failed to fetch listing page.")
        return

    article_items = parse_listing_html(html)
    print(f"🔗 Found {len(article_items)} article(s) on listing page.")

    if not article_items:
        print("⛔ No article URLs found.")
        return

    # Filter out already-scraped URLs
    new_items = []
    for url, title, date in article_items:
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
