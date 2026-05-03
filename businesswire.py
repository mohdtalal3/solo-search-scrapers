import json
import os
import time
from datetime import datetime

import requests
from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://www.businesswire.com"
NEWSROOM_URL = (
    "https://www.businesswire.com/newsroom"
    "?region=1000489"
    "&subject=1000004%7C1000007%7C1000009%7C1000011%7C1000015%7C1050041"
    "&industry=1000048%7C1000051%7C1000107%7C1000150%7C1000084%7C1000162%7C1050101%7C1000178"
    "&language=en"
)
SOURCE_NAME = "BUSINESS_WIRE"
SCRAPER_ID = 31
COMPANY_CONFIGS = [
    {
        "label": "Headliners",
        "company_id": os.getenv("HEADLINERS_COMPANY_ID"),
    },
    {
        "label": "Middlesex Partnership",
        "company_id": os.getenv("MIDDLESEX_PARTNERSHIP_COMPANY_ID"),
    },
]
MAX_PAGES = 6

SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
SCRAPPEY_PROXY_COUNTRY = "UnitedKingdom"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ----------------------------------------------------------
# Scrappey fetch (for listing pages — JS-rendered)
# ----------------------------------------------------------
def fetch_with_scrappey(url, max_retries=3):
    scrappey_api_key = os.getenv("SCRAPPEY_API_KEY")
    if not scrappey_api_key:
        raise RuntimeError("SCRAPPEY_API_KEY not set")

    payload = {
        "cmd": "request.get",
        "requestType": "request",
        "url": url,
        "proxyCountry": SCRAPPEY_PROXY_COUNTRY,
        "premiumProxy": True,
    }

    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = requests.post(
                f"{SCRAPPEY_API_URL}?key={scrappey_api_key}",
                json=payload,
                timeout=90,
            )
            resp.raise_for_status()
            data = resp.json()
            solution = data.get("solution", {})

            status_code = solution.get("statusCode")
            if status_code and status_code != 200:
                print(f"❌ Scrappey returned status {status_code} for {url}")
                return None

            if data.get("data") == "error" or not solution.get("verified", False):
                raise RuntimeError(data.get("error", "Unknown Scrappey error"))

            html = solution.get("response") or ""
            # with open("scrappey_debug.html", "w", encoding="utf-8") as f:
            #     f.write(html)
            # print(f"  💾 Scrappey response saved to scrappey_debug.html ({len(html)} chars)")
            return html

        except (requests.RequestException, RuntimeError) as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


# ----------------------------------------------------------
# curl_cffi fetch with proxy (for individual article pages)
# ----------------------------------------------------------
def fetch_url(url, max_retries=3):
    proxy = os.getenv("SCRAPER_PROXY")
    proxies = {"http": proxy, "https": proxy} if proxy else None
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = cffi_requests.get(
                url,
                headers=HEADERS,
                proxies=proxies,
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


# ----------------------------------------------------------
# Parse listing page → list of (full_url, title, date_str)
# ----------------------------------------------------------
def parse_listing_html(html):
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "/news/home/" not in href:
            continue

        # Filter to English articles only — language code is parts[4]
        # e.g. /news/home/20260430140369/en/Title-Slug
        parts = href.rstrip("/").split("/")
        # parts: ['', 'news', 'home', '<id>', '<lang>', ...]
        lang = parts[4] if len(parts) > 4 else ""
        if lang != "en":
            continue

        full_url = BASE_URL + href if href.startswith("/") else href
        if full_url in seen:
            continue
        seen.add(full_url)

        # Title from h2 inside the link
        h2 = a.find("h2")
        title = h2.get_text(" ", strip=True) if h2 else ""

        results.append((full_url, title))
        print(f"    🔗 {full_url}")
        print(f"       📰 {title[:60]}")

    return results


# ----------------------------------------------------------
# Fetch all listing pages (pages 1–MAX_PAGES)
# ----------------------------------------------------------
def fetch_all_listings():
    all_items = []
    for page in range(1, MAX_PAGES + 1):
        url = f"{NEWSROOM_URL}&page={page}"
        print(f"  📄 Fetching listing page {page}: {url}")
        html = fetch_with_scrappey(url)
        if not html:
            print(f"  ⚠️  Empty response for page {page}, stopping.")
            break
        items = parse_listing_html(html)
        print(f"  📋 Page {page}: {len(items)} English article(s) found.")
        if not items:
            print(f"  ℹ️  No items on page {page}, stopping pagination.")
            break
        all_items.extend(items)
    return all_items


# ----------------------------------------------------------
# Scrape an individual article page
# ----------------------------------------------------------
def scrape_article(url, fallback_title=""):
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title — try <h1>, fall back to og:title, then listing title
    h1 = soup.select_one("h1")
    if h1:
        title = h1.get_text(" ", strip=True)
    else:
        og = soup.find("meta", property="og:title")
        title = og["content"].strip() if og and og.get("content") else fallback_title

    # Date — from <meta property="article:published_time"> or og:updated_time
    date = ""
    for prop in ("article:published_time", "og:updated_time", "article:modified_time"):
        meta = soup.find("meta", property=prop)
        if meta and meta.get("content"):
            date = meta["content"][:19].replace("T", "T")
            break
    if not date:
        # fallback: datePublished in JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, list):
                    data = data[0]
                dp = data.get("datePublished", "")
                if dp:
                    date = dp[:19]
                    break
            except (json.JSONDecodeError, AttributeError):
                continue

    # Body — target the press release story div only
    release_div = soup.select_one("#bw-release-story")
    if not release_div:
        release_div = soup.select_one("div.bw-release-container")
    if release_div:
        for tag in release_div.select("script, style, .bw-related-news, .bw-social-sharing, nav"):
            tag.decompose()
        text = release_div.get_text(" ", strip=True)
    else:
        text = ""

    return {
        "url": url,
        "title": title,
        "text": text,
        "date": date,
        "scraper_id": SCRAPER_ID,
    }


# ----------------------------------------------------------
# Slug helper for URL deduplication
# ----------------------------------------------------------
def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------
def main():
    print("🔍 Fetching Business Wire newsroom listings...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    all_items = fetch_all_listings()
    print(f"\n🔗 Total articles found across all pages: {len(all_items)}")

    # Deduplicate listing results
    deduped = []
    dedup_seen = set()
    for full_url, title in all_items:
        if full_url not in dedup_seen:
            dedup_seen.add(full_url)
            deduped.append((full_url, title))
    print(f"🔗 After deduplication: {len(deduped)} unique article(s).")

    new_items = []
    for full_url, title in deduped:
        slug = url_slug(full_url)
        if full_url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_items.append((full_url, title))
        seen_slugs.add(slug)

    if not new_items:
        print("\n⛔ No new articles found.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

    scraped = []
    for full_url, fallback_title in new_items:
        print(f"  Scraping: {full_url}")
        article = scrape_article(full_url, fallback_title)
        if not article:
            continue
        scraped.append(article)
        print(f"  ✅ {article['title'][:60]}...")

    if not scraped:
        print("\n⛔ No articles scraped successfully.")
        return

    print(f"\n🆕 Found {len(scraped)} new article(s) in total.")

    # Insert once per active company
    for config in COMPANY_CONFIGS:
        if not is_subscription_active(SCRAPER_ID, config["company_id"]):
            print(f"⏭️  Skipping {config['label']} — subscription inactive")
            continue
        articles = [{**a, "company_id": config["company_id"]} for a in scraped]
        inserted_count = insert_articles(articles)
        print(f"✅ Inserted {inserted_count} articles for {config['label']}")


if __name__ == "__main__":
    main()
