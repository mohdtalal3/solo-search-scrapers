import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup
from curl_cffi import requests
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles

load_dotenv()

BASE_URL = "https://www.newswire.ca"
LISTING_URLS = [
    "https://www.newswire.ca/news-releases/business-technology-latest-news/data-analytics-list",
    "https://www.newswire.ca/news-releases/consumer-technology-latest-news/artificial-intelligence-list",
    "https://www.newswire.ca/news-releases/business-technology-latest-news/computer-software-list",
    "https://www.newswire.ca/news-releases/consumer-technology-latest-news/cloud-computing-internet-of-things-list",
    "https://www.newswire.ca/news-releases/business-technology-latest-news/high-tech-security-list",
    "https://www.newswire.ca/news-releases/financial-services-latest-news/venture-capital-list",
    "https://www.newswire.ca/news-releases/financial-services-latest-news/accounting-news-issues-list",
    "https://www.newswire.ca/news-releases/general-business-latest-news/personnel-announcements-list",
    "https://www.newswire.ca/news-releases/general-business-latest-news/corporate-expansion-list",
]
SOURCE_NAME = "NEWSWIRE_CA"
SCRAPER_ID = 83
COMPANY_ID = os.getenv("TALENT_TO_HIRE")
MAX_THREADS = 5

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-language": "en-US,en;q=0.9,en-CA;q=0.8",
    "cache-control": "no-cache",
    "pragma": "no-cache",
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
# HTTP fetch (curl_cffi + proxy)
# ----------------------------------------------------------
def fetch_url(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(
                url,
                headers=HEADERS,
                proxies=PROXIES,
                impersonate="chrome131",
                timeout=30,
            )
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} [{url[:60]}]: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


# ----------------------------------------------------------
# Parse "Sep 15, 2026, 13:06 ET" → UTC ISO string
# ----------------------------------------------------------
def parse_date(date_str):
    if not date_str:
        return None
    cleaned = date_str.strip()
    # Strip timezone suffix (ET/EDT/EST/PT/CT/MT/GMT/BST/UTC)
    cleaned = re.sub(r"\s+(ET|EDT|EST|PT|PDT|PST|CT|CDT|CST|MT|MDT|MST|GMT|BST|UTC)$", "", cleaned)
    # Time-only e.g. "03:13" — same-day release, use today's ET date
    m = re.match(r"^(\d{1,2}):(\d{2})$", cleaned)
    if m:
        et_today = (datetime.now(timezone.utc) - timedelta(hours=4)).date()
        dt = datetime(et_today.year, et_today.month, et_today.day,
                      int(m.group(1)), int(m.group(2)))
        return (dt + timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%S")
    for fmt in ("%b %d, %Y, %H:%M", "%B %d, %Y, %H:%M", "%b %d, %Y %H:%M", "%B %d, %Y %H:%M"):
        try:
            dt = datetime.strptime(cleaned, fmt)
            # Times are ET — approximate to UTC (+4h)
            return (dt + timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return None


# ----------------------------------------------------------
# Parse listing page → [{url, date, title}, ...]
# ----------------------------------------------------------
def parse_listing_page(html):
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen = set()

    for a in soup.select("a.newsreleaseconsolidatelink[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full_url = href if href.startswith("http") else BASE_URL + href
        # Drop fragment / query variants
        full_url = full_url.split("#")[0]
        if full_url in seen:
            continue
        seen.add(full_url)

        h3 = a.find("h3")
        title = ""
        date_str = ""
        if h3:
            small = h3.find("small")
            if small:
                date_str = small.get_text(" ", strip=True)
                small.extract()
            title = h3.get_text(" ", strip=True)

        results.append({
            "url": full_url,
            "date": parse_date(date_str),
            "date_str": date_str,
            "title": title,
        })

    return results


# ----------------------------------------------------------
# Scrape a single article page
# ----------------------------------------------------------
def scrape_article(entry):
    url = entry["url"]
    html = fetch_url(url)
    if not html:
        print(f"❌ Failed to fetch {url}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title — <h1> inside .detail-headline, fall back to og:title / listing title
    h1 = soup.select_one("div.detail-headline h1") or soup.select_one("h1")
    if h1:
        title = h1.get_text(" ", strip=True)
    else:
        og = soup.find("meta", property="og:title")
        title = og["content"].strip() if og and og.get("content") else entry["title"]

    # Date — meta tags first, then "News provided by" <p class="mb-no">, then listing date
    date = ""
    for prop in ("article:published_time", "og:updated_time", "article:modified_time"):
        meta = soup.find("meta", property=prop)
        if meta and meta.get("content"):
            date = meta["content"][:19]
            break
    if not date:
        mb_no = soup.select_one("header.release-header p.mb-no")
        if mb_no:
            date = parse_date(mb_no.get_text(" ", strip=True)) or ""
    if not date:
        date = entry["date"] or ""

    # Body — first content column inside section.release-body
    body_col = soup.select_one("section.release-body div.col-lg-10")
    if not body_col:
        body_col = soup.select_one("section.release-body")
    if not body_col:
        print(f"⚠️  Could not find body content for {url}")
        return None

    for tag in body_col.select(
        "script, style, iframe, img, figure, figcaption, "
        ".inline-gallery-container, .continue-reading, .modal"
    ):
        tag.decompose()

    blocks = [
        el.get_text(" ", strip=True)
        for el in body_col.find_all(["p", "li"])
        if el.get_text(strip=True)
    ]
    text = "\n\n".join(blocks) if blocks else " ".join(body_col.get_text(" ", strip=True).split())

    return {
        "url": url,
        "title": title,
        "text": text,
        "date": date,
        "lastmod": date,
        "scraper_id": SCRAPER_ID,
    }


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------
def main():
    print("🔍 Newswire.ca (Data & Analytics) scraper starting...")

    saved_timestamp = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)
    # Normalize to naive ISO for lexical comparison with parsed dates
    if saved_timestamp:
        saved_timestamp = saved_timestamp[:19]
    print(f"🗄️  Saved timestamp: {saved_timestamp or 'None (first run)'}")

    # Phase 1: fetch first page of each listing (threaded), dedupe by URL
    entries_by_url = {}

    def fetch_listing(listing_url):
        html = fetch_url(listing_url)
        return listing_url, html

    print(f"\n🧵 Fetching {len(LISTING_URLS)} listing page(s) with {MAX_THREADS} threads...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(fetch_listing, u): u for u in LISTING_URLS}
        for future in as_completed(futures):
            listing_url, html = future.result()
            if not html:
                print(f"⛔ Could not fetch listing page, skipping: {listing_url}")
                continue
            entries = parse_listing_page(html)
            print(f"📋 {len(entries)} article(s): ...{listing_url.rsplit('/', 1)[-1]}")
            for e in entries:
                entries_by_url.setdefault(e["url"], e)

    entries = list(entries_by_url.values())
    print(f"\n🔗 Total unique articles across all lists: {len(entries)}")

    if not entries:
        print("⛔ No articles found.")
        return

    dated = [e for e in entries if e["date"]]
    dated.sort(key=lambda e: e["date"], reverse=True)
    newest_timestamp = dated[0]["date"] if dated else None

    # First run — save timestamp only, don't scrape
    if saved_timestamp is None:
        print("🟢 First run detected — NOT scraping any articles.")
        if newest_timestamp:
            update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
            print("🕒 Saved latest timestamp:", newest_timestamp)
        return

    print("Previously saved timestamp:", saved_timestamp)

    new_entries = [e for e in entries if e["date"] and e["date"] > saved_timestamp]
    if not new_entries:
        print("⛔ No new articles found.")
        if newest_timestamp and newest_timestamp > saved_timestamp:
            update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        return

    print(f"🆕 {len(new_entries)} new article(s) to scrape.")

    # Phase 2: threaded article scraping
    articles = []
    print(f"\n🧵 Scraping {len(new_entries)} article(s) with {MAX_THREADS} threads...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_entry = {executor.submit(scrape_article, e): e for e in new_entries}
        for future in as_completed(future_to_entry):
            entry = future_to_entry[future]
            try:
                article = future.result()
            except Exception as e:
                print(f"  ⚠️  Exception scraping {entry['url']}: {e}")
                continue
            if not article:
                print(f"  ⚠️  Failed to scrape: {entry['url']}")
                continue
            if not article.get("date"):
                print(f"  ⚠️  No timestamp for: {entry['url']}")
                continue
            if article["date"] <= saved_timestamp:
                print(f"  ⏭️  Old article, skipping: {article['title'][:60]}")
                continue
            articles.append(article)
            print(f"  ✅ {article['title'][:70]}")

    if not articles:
        print("⛔ No articles scraped successfully.")
        return

    company_articles = [dict(a, company_id=COMPANY_ID) for a in articles]
    inserted = insert_articles(company_articles)
    print(f"✅ Inserted {inserted} article(s)")

    if newest_timestamp:
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
