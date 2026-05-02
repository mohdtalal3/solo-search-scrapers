import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles

load_dotenv()

SOURCE_NAME = "THE_GROCER"
SCRAPER_ID = 34
COMPANY_ID = os.getenv("HEADLINERS_COMPANY_ID")

SITEMAP_NS = {"sm": "http://www.google.com/schemas/sitemap/0.84"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def get_proxies():
    proxy = os.getenv("SCRAPER_PROXY")
    return {"http": proxy, "https": proxy} if proxy else None


def fetch_url(url, max_retries=3):
    proxies = get_proxies()
    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = requests.get(url, headers=HEADERS, proxies=proxies, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed to fetch {url}: {e}")
                return None


def get_sitemap_url():
    year = datetime.now().year
    print(f"📅 Latest year: {year}")
    return f"https://www.thegrocer.co.uk/googlesitemap.aspx?year={year}"


def parse_sitemap(xml_content):
    """Return list of (loc, lastmod_str) sorted newest first."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"❌ XML parse error: {e}")
        return []

    entries = []
    for url_el in root.findall("sm:url", SITEMAP_NS):
        loc_el = url_el.find("sm:loc", SITEMAP_NS)
        lastmod_el = url_el.find("sm:lastmod", SITEMAP_NS)
        if loc_el is None or lastmod_el is None:
            continue
        entries.append((loc_el.text.strip(), lastmod_el.text.strip()))

    # Sort newest first
    entries.sort(key=lambda x: x[1], reverse=True)
    return entries


def normalise_ts(ts_str):
    """Normalise '2026-05-01T14:06:00.867Z' → '2026-05-01T14:06:00'"""
    ts = ts_str.rstrip("Z").split(".")[0]
    return ts


def scrape_article(url, lastmod):
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.find("h1")
    if not h1:
        meta_title = soup.find("meta", property="og:title")
        title = meta_title["content"].strip() if meta_title else ""
    else:
        title = h1.get_text(strip=True)

    if not title:
        return None

    # Body — div.storytext only, stripping ads and inline placeholders
    storytext = soup.find("div", class_="storytext")
    if not storytext:
        return None

    for tag in storytext.select(
        "script, style, iframe, "
        ".story-inlinecontent-placeholder, "
        ".iA-container, "
        ".targetLink"
    ):
        tag.decompose()

    text = storytext.get_text(" ", strip=True)
    text = " ".join(text.split())

    if not text:
        return None

    date = normalise_ts(lastmod)

    return {
        "url": url,
        "date": date,
        "title": title,
        "text": text,
        "lastmod": date,
        "company_id": COMPANY_ID,
        "scraper_id": SCRAPER_ID,
    }


def main():
    saved_timestamp = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)

    sitemap_url = get_sitemap_url()
    print(f"🔍 Fetching sitemap: {sitemap_url}")

    xml_content = fetch_url(sitemap_url)
    if not xml_content:
        print("❌ Could not fetch sitemap.")
        return

    entries = parse_sitemap(xml_content)
    if not entries:
        print("❌ No entries found in sitemap.")
        return

    print(f"📋 Found {len(entries)} entries in sitemap.")

    newest_timestamp = normalise_ts(entries[0][1]) if entries else None

    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT saving any articles.")
        if newest_timestamp:
            print("Saving latest timestamp:", newest_timestamp)
            update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        return

    # ----------------------------
    # SUBSEQUENT RUNS — save new
    # ----------------------------
    print("Previously saved timestamp:", saved_timestamp)

    new_entries = [
        (loc, lastmod)
        for loc, lastmod in entries
        if normalise_ts(lastmod) > saved_timestamp
    ]

    if not new_entries:
        print("⛔ No new articles found.")
        return

    print(f"🆕 Found {len(new_entries)} new article(s) to scrape.")

    all_articles = []
    for loc, lastmod in new_entries:
        print(f"  Scraping: {loc}")
        article = scrape_article(loc, lastmod)
        if not article:
            print(f"  ⚠️  Failed: {loc}")
            continue
        all_articles.append(article)
        print(f"  ✅ {article['title'][:60]}...")

    if not all_articles:
        print("⛔ No articles scraped successfully.")
        return

    inserted_count = insert_articles(all_articles)
    print(f"✅ Inserted {inserted_count} articles into database")

    if newest_timestamp:
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
