import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

SITEMAP_URL = "https://kpmg.com/xx/en/sitemap.xml"
SOURCE_NAME = "KPMG"
SCRAPER_ID = 66
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
    if not lastmod_str:
        return None
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
            resp = requests.get(
                url,
                headers=HEADERS,
                proxies=PROXIES,
                timeout=60,
            )
            resp.raise_for_status()
            return resp
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"[KPMG] Failed to fetch {url}: {e}")
                return None
            time.sleep(2 ** attempt)
    return None


def parse_sitemap(xml_content):
    """Parse sitemap XML and return list of (url, lastmod) tuples."""
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_content)
    entries = []

    for url_elem in root.findall("sm:url", NS):
        loc = url_elem.find("sm:loc", NS)
        lastmod = url_elem.find("sm:lastmod", NS)

        if loc is None:
            continue

        url = loc.text.strip()

        # Only include insights articles
        if "/our-insights/" not in url:
            continue

        lastmod_str = lastmod.text if lastmod is not None else ""
        lastmod_normalized = parse_lastmod(lastmod_str)

        if lastmod_normalized:
            entries.append({"url": url, "lastmod": lastmod_normalized})

    # Sort by lastmod descending (newest first)
    entries.sort(key=lambda x: x["lastmod"], reverse=True)
    return entries


def scrape_article(url):
    """Scrape individual article page."""
    resp = fetch_url(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.content, "html.parser")

    # Only scrape real articles — hub/listing pages lack a hero title
    hero_title = soup.select_one(".cmp-hero-csi__title")
    if not hero_title:
        return None

    title = hero_title.get_text(strip=True)

    # Extract content from the main article body
    # KPMG wraps content in .cmp-column-control rows; the main text column is first
    content_parts = []

    # Remove nav, footer, related teasers, contact cards, download blocks, embeds
    for noise in soup.select("nav, footer, .cmp-teaser, .cmp-contact-card, .cmp-download, .cmp-embed, .cmp-social-share, .cmp-breadcrumb"):
        noise.decompose()

    # Collect text from remaining .cmp-text and .cmp-title__text elements
    for elem in soup.select(".cmp-text, .cmp-title__text"):
        text = " ".join(elem.get_text(" ", strip=True).split())
        if text and len(text) > 30:
            content_parts.append(text)

    content = "\n\n".join(content_parts)

    if not title or len(content) < 200:
        return None

    return {"title": title, "content": content}


def main():
    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print(f"[KPMG] Subscription not active, skipping.")
        return

    print("[KPMG] Starting KPMG scraper...")

    # Fetch sitemap
    sitemap_resp = fetch_url(SITEMAP_URL)
    if not sitemap_resp:
        print("[KPMG] Failed to fetch sitemap")
        return

    # Parse sitemap
    entries = parse_sitemap(sitemap_resp.content)
    print(f"[KPMG] Found {len(entries)} insights articles")

    if not entries:
        return

    # Get latest timestamp from DB
    latest_timestamp = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)
    print(f"[KPMG] Latest timestamp: {latest_timestamp}")

    # Filter entries newer than latest timestamp
    new_entries = [
        e for e in entries if latest_timestamp is None or e["lastmod"] > latest_timestamp
    ]
    print(f"[KPMG] New articles to scrape: {len(new_entries)}")

    # First run - only update timestamp, don't insert
    if latest_timestamp is None:
        if new_entries:
            newest_timestamp = new_entries[0]["lastmod"]
            update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
            print(f"[KPMG] First run - timestamp updated to {newest_timestamp}")
        return

    # Scrape new articles
    articles_to_insert = []
    newest_timestamp = new_entries[0]["lastmod"] if new_entries else None

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_entry = {
            executor.submit(scrape_article, entry["url"]): entry for entry in new_entries
        }

        for future in as_completed(future_to_entry):
            entry = future_to_entry[future]
            try:
                article = future.result()
                if article:
                    articles_to_insert.append({
                        "company_id": COMPANY_ID,
                        "url": entry["url"],
                        "title": article["title"],
                        "text": article["content"],
                        "date": entry["lastmod"],
                    })
                    print(f"[KPMG] Scraped: {article['title'][:80]}...")
            except Exception as e:
                print(f"[KPMG] Error scraping {entry['url']}: {e}")

    # Insert articles
    if articles_to_insert:
        insert_articles(articles_to_insert, COMPANY_ID, SCRAPER_ID)
        print(f"[KPMG] Inserted {len(articles_to_insert)} articles")

    # Update timestamp
    if newest_timestamp:
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        print(f"[KPMG] Timestamp updated to {newest_timestamp}")

    print("[KPMG] Done.")


if __name__ == "__main__":
    main()
