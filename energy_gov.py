import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

SOURCE_NAME = "ENERGY_GOV"
SCRAPER_ID = 43
COMPANY_ID = os.getenv("NET_ZERO_SEARCH_COMPANY_ID")

BASE_URL = "https://www.energy.gov"
API_BASE = (
    "https://www.energy.gov/api/v1/search"
    "?page={page}&sort_by=date"
    "&f%5B0%5D=offices_rest%3AEnergy.gov"
    "&f%5B1%5D=offices_rest%3AOffice+of+Clean+Energy+Demonstrations"
    "&f%5B2%5D=offices_rest%3AClean+Energy+Infrastructure"
    "&f%5B3%5D=offices_rest%3AFunding+Opportunities"
    "&f%5B4%5D=offices_rest%3AGrid+Deployment+Office"
    "&f%5B5%5D=offices_rest%3AHydrogen+and+Fuel+Cell+Technologies+Office"
    "&f%5B6%5D=offices_rest%3ASolar+Energy+Technologies+Office"
    "&f%5B7%5D=offices_rest%3AWind+Energy+Technologies+Office"
    "&f%5B8%5D=offices_rest%3AEnergy+Storage+Grand+Challenge"
    "&f%5B9%5D=offices_rest%3AOffice+of+Electricity"
    "&f%5B10%5D=bundle_alias%3APress+Releases"
    "&f%5B11%5D=bundle_alias%3AArticle"
    "&f%5B12%5D=bundle_alias%3ADocument"
)
MAX_PAGES = 2

PROXIES = None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.energy.gov/search",
}


def fetch_listing() -> list[dict]:
    """Call the API across MAX_PAGES pages and return list of {url, title, raw_date}."""
    items = []
    for page in range(MAX_PAGES):
        url = API_BASE.format(page=page)
        try:
            resp = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  ❌ Failed to fetch API page {page}: {e}")
            break

        rows = data.get("rows", [])
        if not rows:
            break

        for row in rows:
            title_html = row.get("title", "")
            soup = BeautifulSoup(title_html, "html.parser")
            a_tag = soup.find("a")
            if not a_tag:
                continue
            path = a_tag.get("href", "").strip()
            if not path:
                continue
            article_url = BASE_URL + path if path.startswith("/") else path
            title = row.get("titleUnion", a_tag.get_text(strip=True))
            raw_date = row.get("date", "")
            items.append({"url": article_url, "title": title, "raw_date": raw_date})

        time.sleep(1)

    return items


def parse_date(raw: str) -> str:
    """Parse 'May 14, 2026' → '2026-05-14T00:00:00'."""
    try:
        dt = datetime.strptime(raw.strip(), "%B %d, %Y")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return ""


def scrape_body(url: str) -> str:
    """Fetch an energy.gov article page and extract body text only."""
    try:
        time.sleep(1)
        resp = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ❌ Failed to fetch {url}: {e}")
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    article_el = soup.select_one("article")
    if article_el:
        for tag in article_el.select(
            "script, style, nav, .press-release-buttons, .tags, "
            ".read-time, .primary-office, .summary, .beneath-title"
        ):
            tag.decompose()
        return article_el.get_text(" ", strip=True)

    main_el = soup.select_one("#block-main-page-content")
    if main_el:
        for tag in main_el.select("script, style, nav"):
            tag.decompose()
        return main_el.get_text(" ", strip=True)

    return ""


def main():
    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping Energy.gov — subscription is inactive")
        return

    print("🔍 Scraping Energy.gov (Clean Energy Press Releases & Articles)...")

    recent_urls = set(get_recent_article_urls(SCRAPER_ID, limit=500))

    items = fetch_listing()
    print(f"  📰 {len(items)} article(s) found across {MAX_PAGES} page(s).")
    if not items:
        print("⛔ No items found.")
        return

    new_items = [it for it in items if it["url"] not in recent_urls]
    if not new_items:
        print("⛔ No new articles since last run.")
        return

    print(f"  🆕 {len(new_items)} new article(s) to scrape.")

    articles = []

    def scrape_one(item: dict):
        print(f"  Scraping: {item['url']}")
        body = scrape_body(item["url"])
        if not body:
            return None
        result = {
            "url": item["url"],
            "title": item["title"],
            "text": body,
            "date": parse_date(item["raw_date"]),
            "scraper_id": SCRAPER_ID,
            "company_id": COMPANY_ID,
        }
        print(f"  ✅ {result['title'][:70]}")
        return result

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_one, item): item for item in new_items}
        for future in as_completed(futures):
            result = future.result()
            if result:
                articles.append(result)

    if not articles:
        print("⛔ No articles scraped successfully.")
        return

    inserted = insert_articles(articles)
    print(f"✅ Inserted {inserted} articles into database.")


if __name__ == "__main__":
    main()
