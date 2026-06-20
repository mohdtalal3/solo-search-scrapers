import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

SITEMAP_URL = "https://www.inoapps.com/sitemap.xml"
SOURCE_NAME = "INOAPPS"
SCRAPER_ID = 64
COMPANY_ID = os.getenv("ERP_RECRUIT_COMPANY_ID")

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
    """Normalise lastmod to YYYY-MM-DDTHH:MM:SS. Sitemap gives YYYY-MM-DD."""
    s = lastmod_str.strip()
    if len(s) == 10:
        return s + "T00:00:00"
    return s[:19]


def is_insight_article(url):
    """Return True only for individual insight/blog article URLs."""
    if "/insights/" not in url:
        return False
    path_after = url.split("/insights/")[-1].rstrip("/")
    if not path_after:
        return False
    if path_after.startswith("topic/") or path_after.startswith("author/"):
        return False
    return True


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
    """Extract (url, lastmod) pairs for insight articles only."""
    soup = BeautifulSoup(xml_content, "xml")
    entries = []

    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        lastmod = url_tag.find("lastmod")
        if not loc or not lastmod:
            continue
        article_url = loc.get_text(strip=True)
        if not is_insight_article(article_url):
            continue
        entries.append({
            "url": article_url,
            "lastmod": parse_lastmod(lastmod.get_text(strip=True)),
        })

    entries.sort(key=lambda x: x["lastmod"], reverse=True)
    return entries


def scrape_article(entry):
    """Fetch and parse a single Inoapps insight article."""
    url = entry["url"]
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.select("script, style, iframe"):
        tag.decompose()

    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""

    body_span = soup.select_one("#hs_cos_wrapper_post_body")
    if body_span:
        text = " ".join(body_span.get_text(" ", strip=True).split())
    else:
        text = ""

    return {
        "url": url,
        "title": title,
        "text": text,
        "date": entry["lastmod"],
        "lastmod": entry["lastmod"],
        "scraper_id": SCRAPER_ID,
    }


def main():
    print("🔍 Fetching Inoapps sitemap...")

    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping Inoapps — subscription is inactive")
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
    print(f"✅ Inserted {inserted} article(s) for ERP Recruit")

    update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
