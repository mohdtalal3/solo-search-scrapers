import json
import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

os.makedirs("logs", exist_ok=True)

_proxy = os.getenv("SCRAPER_PROXY")

BASE_URL = "https://www.businesswire.com"
SOURCE_NAME = "BUSINESS_WIRE"
SCRAPER_ID = 31
MAX_PAGES = 5

COMPANY_CONFIGS = [
    {
        "label": "Headliners",
        "company_id": os.getenv("HEADLINERS_COMPANY_ID"),
        "newsroom_url": (
            "https://www.businesswire.com/newsroom"
            "?region=1000489"
            "&subject=1000004%7C1000007%7C1000009%7C1000011%7C1000015%7C1050041"
            "&industry=1000048%7C1000051%7C1000107%7C1000150%7C1000084%7C1000162%7C1050101%7C1000178"
            "&language=en"
        ),
    },
    {
        "label": "Middlesex Partnership",
        "company_id": os.getenv("MIDDLESEX_PARTNERSHIP_COMPANY_ID"),
        "newsroom_url": (
            "https://www.businesswire.com/newsroom"
            "?region=1000262%7C1000489"
            "&industry=1000048%7C1000150%7C1000084%7C1000178"
            "&subject=1778692%7C1000004%7C1000011%7C1000013%7C1778693%7C1050037"
        ),
    },
    {
        "label": "Net Zero Search",
        "company_id": os.getenv("NET_ZERO_SEARCH_COMPANY_ID"),
        "newsroom_url": (
            "https://www.businesswire.com/newsroom"
            "?language=en"
            "&region=1000490"
            "&industry=1000049%7C1000065%7C1000119%7C1000178%7C1000068"
            "&subject=1778692%7C1000004%7C1050034%7C1050037%7C1000007%7C1000011%7C1000013%7C1778693%7C1000015"
        ),
    },
    {
        "label": "H2 Recruit",
        "company_id": os.getenv("H2_RECRUIT_COMPANY_ID"),
        "newsroom_url": (
            "https://www.businesswire.com/newsroom?industry=1000178&subject=1778692%7C1000007%7C1000011%7C1000013%7C1000015%7C1000004&region=1000283"
        ),
    },
    {
        "label": "VM Search",
        "company_id": os.getenv("VM_SEARCH_COMPANY_ID"),
        "newsroom_url": (
            "https://www.businesswire.com/newsroom"
            "?industry=1000178%7C1000067"
            "&subject=1000004%7C1000007%7C1000011%7C1000013%7C1000015%7C1050041%7C1085196"
            "&region=1000283"

        ),
    },
    {
        "label": "Intune Talent",
        "company_id": os.getenv("INTUNE_TALENT_COMPANY_ID"),
        "newsroom_url": (
            "https://www.businesswire.com/newsroom"
            "?region=1000490"
            "&industry=1000178"
            "&subject=1778692%7C1000004%7C1000007%7C1000011%7C1000013%7C1000015"
        ),
    },
    {
        "label": "1492 Search",
        "company_id": os.getenv("1492_SEARCH_COMPANY_ID"),
        "newsroom_url": (
            "https://www.businesswire.com/newsroom"
            "?industry=1000051%7C1000088%7C1000107"
            "&subject=1000004%7C1000007%7C1000011%7C1000013%7C1778692"
            "&region=1000489"
        ),
    },
    {
        "label": "Time to Hire",
        "company_id": os.getenv("TIME_TO_HIRE_RECRUITMENT_LTD_COMPANY_ID"),
        "newsroom_url": (
            "https://www.businesswire.com/newsroom"
            "?region=1000489"
            "&industry=1000150%7C1000178"
            "&subject=1778692%7C1000007%7C1000011"
        ),
    },
]
HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-language": "en-US,en;q=0.9,fr;q=0.8,af;q=0.7,ar;q=0.6,be;q=0.5,de;q=0.4",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "sec-ch-ua": '"Not=A?Brand";v="99", "Google Chrome";v="131", "Chromium";v="131"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


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
def fetch_all_listings(page, newsroom_url):
    all_items = []
    print(f"  🌐 Fetching newsroom: {newsroom_url[:80]}...")

    for pg in range(1, MAX_PAGES + 1):
        url = f"{newsroom_url}&page={pg}"
        print(f"  📄 Fetching listing page {pg}: {url}")

        for attempt in range(3):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(3000)
                break
            except Exception as e:
                if attempt < 2:
                    print(f"  ⚠️  Retry {attempt + 1}/3 for page {pg}: {e}")
                    page.wait_for_timeout(3000)
                else:
                    print(f"  ❌ Failed to load page {pg}: {e}")
                    break

        screenshot_path = f"logs/bw_page_{pg}.png"
        try:
            page.screenshot(path=screenshot_path, full_page=True, timeout=30000)
            print(f"  📸 Screenshot saved: {screenshot_path}")
        except Exception as e:
            print(f"  ⚠️  Screenshot failed for page {pg}: {e}")

        html = page.content()
        if not html:
            print(f"  ⚠️  Empty response for page {pg}, stopping.")
            break

        items = parse_listing_html(html)
        print(f"  📋 Page {pg}: {len(items)} English article(s) found.")
        if not items:
            print(f"  ℹ️  No items on page {pg}, stopping pagination.")
            break
        all_items.extend(items)

    return all_items


# ----------------------------------------------------------
# Scrape an individual article page
# ----------------------------------------------------------
def scrape_article(page, url, fallback_title=""):
    slug = url_slug(url)
    screenshot_path = f"logs/bw_article_{slug}.png"

    for attempt in range(3):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
            break
        except Exception as e:
            if attempt < 2:
                print(f"  ⚠️  Retry {attempt + 1}/3 for {url[:60]}: {e}")
                page.wait_for_timeout(3000)
            else:
                print(f"  ❌ Failed to load {url}: {e}")
                try:
                    page.screenshot(path=screenshot_path, full_page=True, timeout=15000)
                except Exception:
                    pass
                return None

    for attempt in range(2):
        try:
            page.screenshot(path=screenshot_path, full_page=True, timeout=30000)
            print(f"  📸 Screenshot saved: {screenshot_path}")
            break
        except Exception as e:
            if attempt < 1:
                print(f"  ⚠️  Screenshot retry for {slug[:40]}: {e}")
                page.wait_for_timeout(2000)
            else:
                print(f"  ⚠️  Screenshot failed for {slug[:40]}, skipping image")

    html = page.content()
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
# Run for a single company config
# ----------------------------------------------------------
def run_for_company(config: dict):
    label = config["label"]
    company_id = config["company_id"]
    newsroom_url = config["newsroom_url"]

    if not is_subscription_active(SCRAPER_ID, company_id):
        print(f"\n⏭️  Skipping {label} — subscription is inactive")
        return

    print(f"\n{'='*60}")
    print(f"🏢 Running for: {label}")
    print(f"{'='*60}")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    cdp_url = os.getenv("TWOCAPTCHA_CDP_URL")
    if not cdp_url:
        print("❌ TWOCAPTCHA_CDP_URL not set in .env")
        return

    with sync_playwright() as p:
        print("  🔧 Connecting to 2Captcha Browser API...")
        browser = p.chromium.connect_over_cdp(cdp_url)
        context = browser.contexts[0]
        page = context.new_page()

        BLOCKED_TYPES = {"image", "media", "font", "stylesheet"}
        def _block_resources(route):
            if route.request.resource_type in BLOCKED_TYPES:
                route.abort()
            else:
                route.continue_()
        page.route("**/*", _block_resources)

        print("  ✅ Warmup: visiting base URL...")
        page.goto("https://www.businesswire.com", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        print(f"  ✅ Warmup done: {page.title()}")

        all_items = fetch_all_listings(page, newsroom_url)
        print(f"\n🔗 Total articles found across all pages: {len(all_items)}")

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
            browser.close()
            return

        print(f"  🆕 {len(new_items)} new article(s) to scrape.")

        scraped = []
        for full_url, fallback_title in new_items:
            print(f"  Scraping: {full_url}")
            article = scrape_article(page, full_url, fallback_title)
            if not article:
                continue
            scraped.append(article)
            print(f"  ✅ {article['title'][:60]}...")

        if not scraped:
            print("\n⛔ No articles scraped successfully.")
            browser.close()
            return

        print(f"\n🆕 Found {len(scraped)} new article(s) in total.")

        articles = [{**a, "company_id": company_id} for a in scraped]
        inserted_count = insert_articles(articles)
        print(f"✅ Inserted {inserted_count} articles for {label}")

        browser.close()


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------
def main():
    for config in COMPANY_CONFIGS:
        run_for_company(config)
        time.sleep(5)


if __name__ == "__main__":
    main()
