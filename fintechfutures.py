import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

SOURCE_NAME = "FINTECHFUTURES"
SCRAPER_ID = 53
MAX_THREADS = 5

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

COMPANY_CONFIGS = [
    {
        "label": "H2 Recruit",
        "company_id": os.getenv("H2_RECRUIT_COMPANY_ID"),
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}


def get_sitemap_url():
    """Build the current month's sitemap URL dynamically."""
    now = datetime.utcnow()
    month_name = now.strftime("%B").lower()   # e.g. "june"
    year = now.strftime("%Y")                 # e.g. "2026"
    return f"https://www.fintechfutures.com/article/archive/{year}/{month_name}.xml"


SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"


# ----------------------------------------------------------
# Scrappey fetch (for XML sitemap — blocked by 403 on direct)
# ----------------------------------------------------------
def fetch_with_scrappey(url, max_retries=3):
    api_key = os.getenv("SCRAPPEY_API_KEY")
    if not api_key:
        raise RuntimeError("SCRAPPEY_API_KEY not set")

    payload = {
        "cmd": "request.get",
        "requestType": "request",
        "url": url,
        "premiumProxy": True,
        "proxyCountry": "UnitedKingdom",
        "retries": 1,
        "automaticallySolveCaptcha": True,
    }

    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = requests.post(
                f"{SCRAPPEY_API_URL}?key={api_key}",
                json=payload,
                timeout=90,
            )
            resp.raise_for_status()
            data = resp.json()
            solution = data.get("solution", {})

            status_code = solution.get("statusCode")
            if status_code and status_code != 200:
                raise RuntimeError(f"Scrappey returned status {status_code}")

            content = solution.get("response") or ""
            if not content:
                raise RuntimeError("Empty Scrappey response")
            return content
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


# ----------------------------------------------------------
# HTTP fetch (with proxy)
# ----------------------------------------------------------
def fetch_url(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} [{url[:70]}]: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


# ----------------------------------------------------------
# Phase 1: Parse sitemap XML → [(url, lastmod), ...]
# Stops collecting when lastmod <= saved_timestamp
# ----------------------------------------------------------
def collect_links(saved_timestamp):
    sitemap_url = get_sitemap_url()
    print(f"📡 Fetching sitemap: {sitemap_url}")

    xml = fetch_with_scrappey(sitemap_url)
    if not xml:
        print("⛔ Could not fetch sitemap.")
        return []

    soup = BeautifulSoup(xml, "xml")
    collected = []

    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        lastmod = url_tag.find("lastmod")
        if not loc or not lastmod:
            continue

        article_url = loc.get_text(strip=True)
        # Normalise: strip trailing Z, keep as YYYY-MM-DDTHH:MM:SS
        timestamp = lastmod.get_text(strip=True).rstrip("Z")[:19]
        collected.append((article_url, timestamp))

    # Sort newest → oldest
    collected.sort(key=lambda x: x[1], reverse=True)

    # Filter out articles already saved
    if saved_timestamp:
        before = len(collected)
        collected = [(url, ts) for url, ts in collected if ts > saved_timestamp]
        print(f"📋 {len(collected)} new article(s) after filtering (removed {before - len(collected)} old).")
    else:
        print(f"📋 {len(collected)} article(s) found.")

    for url, ts in collected:
        print(f"    🔗 [{ts}] {url}")

    return collected


# ----------------------------------------------------------
# Phase 2: Scrape a single article page
# ----------------------------------------------------------
def scrape_article(url, lastmod):
    html = fetch_with_scrappey(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title
    title_tag = soup.select_one("span.ArticleBase-LargeTitle")
    if not title_tag:
        title_tag = soup.select_one("h1.ArticleBase-HeaderTitle")
    title = title_tag.get_text(" ", strip=True) if title_tag else ""

    if not title:
        og = soup.find("meta", property="og:title")
        title = og["content"].strip() if og and og.get("content") else ""

    # Body text — try several selectors
    body_div = (
        soup.select_one("div.ArticleBase-Body")
        or soup.select_one("div.ArticleBase-Content")
        or soup.select_one("article")
        or soup.select_one("div[data-testid='article-body']")
    )
    if body_div:
        for tag in body_div.select("script, style, iframe, aside, .ArticleBase-Topics, .EventsPromotions"):
            tag.decompose()
        text = " ".join(body_div.get_text(" ", strip=True).split())
    else:
        text = ""

    return {
        "url": url,
        "title": title,
        "text": text,
        "date": lastmod,
        "lastmod": lastmod,
        "scraper_id": SCRAPER_ID,
    }


# ----------------------------------------------------------
# Phase 2: Threaded article scraping
# ----------------------------------------------------------
def scrape_articles_threaded(links, saved_timestamp):
    articles = []

    # Links are already sorted newest→oldest and filtered; newest is first
    newest_timestamp = links[0][1] if links else None

    print(f"\n🧵 Scraping {len(links)} article(s) with {MAX_THREADS} threads...")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_item = {
            executor.submit(scrape_article, url, lastmod): (url, lastmod)
            for url, lastmod in links
        }

        for future in as_completed(future_to_item):
            url, lastmod = future_to_item[future]
            try:
                article = future.result()
            except Exception as e:
                print(f"  ⚠️  Exception scraping {url}: {e}")
                continue

            if not article:
                print(f"  ⚠️  Failed to scrape: {url}")
                continue

            articles.append(article)
            print(f"  ✅ {article['title'][:70]}...")

    return articles, newest_timestamp


# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
def main():
    company_id = COMPANY_CONFIGS[0]["company_id"]
    saved_timestamp = get_latest_timestamp(SCRAPER_ID, company_id)

    print("🔍 FinTech Futures scraper starting...")
    print(f"🗄️  Saved timestamp: {saved_timestamp or 'None (first run)'}")

    # Phase 1: collect links from sitemap
    links = collect_links(saved_timestamp)
    print(f"\n🔗 Total articles to scrape: {len(links)}")

    if not links:
        print("⛔ No articles found.")
        return

    # Phase 2: threaded scraping
    all_articles, newest_timestamp = scrape_articles_threaded(links, saved_timestamp)

    # Save results per company
    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        label = config["label"]

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        if not is_subscription_active(SCRAPER_ID, company_id):
            print(f"⏭️  Skipping {label} — subscription is inactive")
            continue

        saved_ts = get_latest_timestamp(SCRAPER_ID, company_id)

        # First run — save timestamp only
        if saved_ts is None:
            print("🟢 First run detected — NOT saving any articles.")
            if newest_timestamp:
                update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
                print("🕒 Saved latest timestamp:", newest_timestamp)
            continue

        print("Previously saved timestamp:", saved_ts)

        new_articles = [a for a in all_articles if a["date"] > saved_ts]

        if not new_articles:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_articles)} new article(s).")

        company_articles = [dict(a, company_id=company_id) for a in new_articles]
        inserted = insert_articles(company_articles)
        print(f"✅ Inserted {inserted} article(s) for {label}")

        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
