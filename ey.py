import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

SITEMAP_URL = "https://www.ey.com/en_uk/sitemap/insights.xml"
SOURCE_NAME = "EY"
SCRAPER_ID = 65
COMPANY_ID = os.getenv("ERP_RECRUIT_COMPANY_ID")

MAX_THREADS = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml, text/xml, */*",
    "Accept-Language": "en-GB,en;q=0.9",
}

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def parse_lastmod(lastmod_str):
    """Convert lastmod to YYYY-MM-DDTHH:MM:SS format."""
    try:
        dt = datetime.fromisoformat(lastmod_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def fetch_url(url, max_retries=3):
    """Fetch URL content with retries."""
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
    """Parse sitemap XML and return list of entries with url and lastmod."""
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_content)
    entries = []

    for url_elem in root.findall("sm:url", NS):
        loc = url_elem.find("sm:loc", NS)
        lastmod = url_elem.find("sm:lastmod", NS)

        if loc is None:
            continue

        url = loc.text.strip()
        lastmod_text = lastmod.text.strip() if lastmod is not None else ""
        timestamp = parse_lastmod(lastmod_text)

        if not timestamp:
            continue

        entries.append({"url": url, "lastmod": timestamp})

    entries.sort(key=lambda x: x["lastmod"], reverse=True)
    return entries


def scrape_article(entry):
    """Scrape a single EY article."""
    url = entry["url"]
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Remove script and style elements
    for tag in soup.select("script, style, iframe"):
        tag.decompose()

    # Extract title - try hero title first, then h1
    title = ""
    hero_title = soup.select_one(".cmp-hero__title h1")
    if hero_title:
        title = hero_title.get_text(" ", strip=True)
    else:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(" ", strip=True)

    # Extract content from rich text containers
    rich_texts = soup.select(".up-rich-text .up-rich-text__container-content")
    if not rich_texts:
        rich_texts = soup.select(".up-rich-text")

    text_parts = []
    for rt in rich_texts:
        text = rt.get_text(" ", strip=True)
        if text:
            text_parts.append(text)

    text = " ".join(text_parts)
    text = " ".join(text.split())  # Normalize whitespace

    return {
        "url": url,
        "title": title,
        "text": text,
        "date": entry["lastmod"],
        "lastmod": entry["lastmod"],
        "scraper_id": SCRAPER_ID,
    }


def main():
    print("🔍 Fetching EY insights sitemap...")

    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping EY — subscription is inactive")
        return

    xml_content = fetch_url(SITEMAP_URL)
    if not xml_content:
        print("⛔ Failed to fetch sitemap.")
        return

    entries = parse_sitemap(xml_content)
    print(f"📋 Found {len(entries)} insight article(s) in sitemap.")

    if not entries:
        print("⛔ No articles found in sitemap.")
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
    print(f"✅ Inserted {inserted} article(s) for ERP Recruit")

    update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
