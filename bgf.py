import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

SITEMAP_URL = "https://www.bgf.co.uk/insight-sitemap.xml"
SOURCE_NAME = "BGF"
SCRAPER_ID = 81
COMPANY_ID = os.getenv("TIME_TO_HIRE_RECRUITMENT_LTD_COMPANY_ID")

MAX_THREADS = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None


def parse_lastmod(lastmod_str):
    """Normalise lastmod to YYYY-MM-DDTHH:MM:SS."""
    s = lastmod_str.strip()
    if len(s) == 10:
        return s + "T00:00:00"
    return s[:19]


def parse_page_date(date_str):
    """Parse '21 August 2026' → '2026-08-21T00:00:00'"""
    try:
        dt = datetime.strptime(date_str.strip(), "%d %B %Y")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def fetch_url(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


def parse_sitemap(xml_content):
    """Extract (url, lastmod) pairs from the insight sitemap."""
    soup = BeautifulSoup(xml_content, "xml")
    entries = []

    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        lastmod = url_tag.find("lastmod")
        if not loc or not lastmod:
            continue
        entries.append({
            "url": loc.get_text(strip=True),
            "lastmod": parse_lastmod(lastmod.get_text(strip=True)),
        })

    entries.sort(key=lambda x: x["lastmod"], reverse=True)
    return entries


def scrape_article(entry):
    """Fetch and parse a single BGF insight article."""
    url = entry["url"]
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    container = soup.find("div", class_="content-insight")
    if not container:
        print(f"  ⚠️  No content-insight container: {url}")
        return None

    for tag in container.select("script, style, iframe, .featured-image"):
        tag.decompose()

    h1 = container.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    date = None
    date_div = container.find("div", class_="tw-opacity-50")
    if date_div:
        date = parse_page_date(date_div.get_text(strip=True))
    if not date:
        date = entry["lastmod"]

    blocks = container.select("div.block-article-content, div.block-article-quote")
    parts = []
    for block in blocks:
        text = " ".join(block.get_text(" ", strip=True).split())
        if text:
            parts.append(text)
    text = " ".join(parts)

    if not title or not text:
        print(f"  ⚠️  Missing title or text: {url}")
        return None

    return {
        "url": url,
        "title": title,
        "text": text,
        "date": date,
        "lastmod": entry["lastmod"],
        "scraper_id": SCRAPER_ID,
    }


def main():
    print("🔍 Fetching BGF insight sitemap...")

    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping BGF — subscription is inactive")
        return

    xml_content = fetch_url(SITEMAP_URL)
    if not xml_content:
        print("⛔ Failed to fetch sitemap.")
        return

    entries = parse_sitemap(xml_content)
    print(f"📋 Found {len(entries)} insight article(s) in sitemap.")

    if not entries:
        print("⛔ No insight articles found.")
        return

    newest_timestamp = entries[0]["lastmod"]
    saved_timestamp = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)

    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT saving any articles.")
        print("Saving latest timestamp:", newest_timestamp)
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        return

    print("Previously saved timestamp:", saved_timestamp)

    new_entries = [e for e in entries if e["lastmod"] > saved_timestamp]

    if not new_entries:
        print("⛔ No new articles found.")
        return

    print(f"🆕 Found {len(new_entries)} new article(s). Scraping...")

    scraped = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(scrape_article, e): e for e in new_entries}
        for future in as_completed(futures):
            result = future.result()
            if result:
                scraped.append(result)
                print(f"  ✅ {result['title'][:70]}...")

    if not scraped:
        print("⛔ No articles scraped successfully.")
        return

    articles = [dict(a, company_id=COMPANY_ID) for a in scraped]
    inserted = insert_articles(articles)
    print(f"✅ Inserted {inserted} article(s) for Time to Hire")

    update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
