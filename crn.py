import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from curl_cffi import requests 
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://www.crn.com"
LISTING_URL = f"{BASE_URL}/news"
SOURCE_NAME = "CRN"
SCRAPER_ID = 75

COMPANY_CONFIGS = [
    {
        "label": "Intune Talent",
        "company_id": os.getenv("INTUNE_TALENT_COMPANY_ID"),
    },
]

MAX_RETRIES = 3
MAX_WORKERS = 5

SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
SCRAPPEY_PROXY_COUNTRY = "UnitedStates"

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


def fetch_with_scrappey(url, max_retries=MAX_RETRIES):
    scrappey_api_key = os.getenv("SCRAPPEY_API_KEY")
    if not scrappey_api_key:
        raise RuntimeError("Please set SCRAPPEY_API_KEY in your environment")

    payload = {
        "cmd": "request.get",
        "url": url,
        "proxyCountry": SCRAPPEY_PROXY_COUNTRY,
        "automaticallySolveCaptcha": True,
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
    """Extract article links and titles from the news listing page."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen_urls = set()

    for card in soup.select("div.article-card-title"):
        link_el = card.find("a", href=True)
        if not link_el:
            continue
        title = link_el.get_text(" ", strip=True)
        article_url = urljoin(BASE_URL, link_el["href"])
        if article_url in seen_urls:
            continue
        seen_urls.add(article_url)
        items.append((article_url, title))

    return items


def scrape_article_text(html):
    """Extract article body text and date from article page HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Extract date from meta tag (present in simple HTML version)
    date = ""
    meta_date = soup.find("meta", attrs={"name": "publisheddate"})
    if meta_date:
        raw_date = meta_date.get("content", "").strip()
        for fmt in (
            "%B %d, %Y, %I:%M %p %Z",
            "%B %d, %Y, %I:%M %p",
            "%B %d, %Y",
            "%b %d, %Y",
        ):
            try:
                dt = datetime.strptime(raw_date, fmt)
                date = dt.strftime("%Y-%m-%dT%H:%M:%S")
                break
            except ValueError:
                continue
        if not date:
            date = raw_date

    # Try rendered structure first (div.default-content-wrapper)
    content_wrapper = soup.select_one("div.default-content-wrapper")
    if content_wrapper:
        date_el = content_wrapper.select_one("div.article-date")
        if date_el:
            raw_date = date_el.get_text(" ", strip=True)
            for fmt in (
                "%B %d, %Y, %I:%M %p %Z",
                "%B %d, %Y, %I:%M %p",
                "%B %d, %Y",
                "%b %d, %Y",
            ):
                try:
                    dt = datetime.strptime(raw_date, fmt)
                    date = dt.strftime("%Y-%m-%dT%H:%M:%S")
                    break
                except ValueError:
                    continue
            if not date:
                date = raw_date
    else:
        # Simple HTML: collect all divs inside main
        main_el = soup.find("main")
        if main_el:
            content_wrapper = main_el
    if not content_wrapper:
        return None, None

    # Remove unwanted elements
    for el in content_wrapper.select(
        ".social-share-container, "
        ".social-share, "
        ".social-share-wrapper, "
        ".sharethis-inline-share-buttons, "
        ".beyondwords-container, "
        ".ad-wrapper, "
        ".right-ad, "
        "#miso-ask-combo, "
        "#miso-explore, "
        ".article-author-container, "
        ".article-category-link, "
        "script, style, iframe, "
        "picture, "
        "h1"
    ):
        el.decompose()

    paragraphs = []
    for el in content_wrapper.find_all(["p", "h2", "h3", "blockquote"]):
        text = el.get_text(" ", strip=True)
        text = " ".join(text.split())
        if text:
            paragraphs.append(text)

    body_text = "\n\n".join(paragraphs) if paragraphs else None
    return body_text, date


def scrape_article(url, fallback_title=""):
    """Fetch an article page and extract body text and date."""
    html = fetch_url(url)
    if not html:
        return None

    text, date = scrape_article_text(html)
    if not text:
        print(f"⚠️  Missing body for {url}")
        return None

    return {
        "url": url,
        "date": date,
        "title": fallback_title,
        "text": text,
        "lastmod": date,
        "scraper_id": SCRAPER_ID,
    }


def main():
    print("🔍 Fetching CRN news listing...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    html = fetch_with_scrappey(LISTING_URL)
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
    for url, title in all_items:
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

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

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
