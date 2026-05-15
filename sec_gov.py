import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles, is_subscription_active

load_dotenv()

SOURCE_NAME = "SEC_GOV"
SCRAPER_ID = 44
COMPANY_ID = os.getenv("NET_ZERO_SEARCH_COMPANY_ID")

BASE_URL = "https://www.sec.gov"
PAGE_SIZE = 100
MAX_PAGES = 5

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

DOC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    "Accept": "text/html,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def build_api_url(filing_date: str, page: int) -> str:
    from_val = (page - 1) * PAGE_SIZE
    url = (
        "https://efts.sec.gov/LATEST/search-index"
        f"?dateRange=custom&category=custom&forms=8-K%2CD"
        f"&startdt={filing_date}&enddt={filing_date}"
        f"&page={page}&from={from_val}"
    )
    return url


def build_doc_url(hit: dict) -> str | None:
    """Construct the SEC archives URL for a filing document."""
    source = hit.get("_source", {})
    ciks = source.get("ciks", [])
    adsh = source.get("adsh", "")
    xsl = source.get("xsl", "")
    hit_id = hit.get("_id", "")

    if not ciks or not adsh:
        return None

    cik = ciks[0].lstrip("0")
    adsh_clean = adsh.replace("-", "")

    # filename from _id: "adsh:filename"
    filename = "primary_doc.xml"
    if ":" in hit_id:
        filename = hit_id.split(":", 1)[1]

    if xsl:
        return f"{BASE_URL}/Archives/edgar/data/{cik}/{adsh_clean}/{xsl}/{filename}"
    else:
        return f"{BASE_URL}/Archives/edgar/data/{cik}/{adsh_clean}/{filename}"


def fetch_page(filing_date: str, page: int) -> list[dict]:
    """Fetch one page of search results and return list of {url, title, date}."""
    url = build_api_url(filing_date, page)
    try:
        resp = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ❌ Failed to fetch page {page}: {e}")
        return []

    hits = data.get("hits", {}).get("hits", [])
    items = []
    for hit in hits:
        source = hit.get("_source", {})
        doc_url = build_doc_url(hit)
        if not doc_url:
            continue

        display_names = source.get("display_names", [])
        company = display_names[0] if display_names else "Unknown"
        form_type = source.get("form", "")
        title = f"{form_type} — {company}" if form_type else company
        file_date = source.get("file_date", "")
        if file_date:
            file_date = f"{file_date}T00:00:00"

        items.append({"url": doc_url, "title": title, "date": file_date})

    return items


def scrape_body(url: str) -> str:
    """Fetch and extract text from an SEC filing document page."""
    try:
        time.sleep(0.5)
        resp = requests.get(url, headers=DOC_HEADERS, proxies=PROXIES, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ❌ Failed to fetch {url}: {e}")
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    for tag in soup.select("script, style"):
        tag.decompose()

    body = soup.find("body")
    if body:
        return body.get_text(" ", strip=True)

    return soup.get_text(" ", strip=True)


def main():
    if not is_subscription_active(SCRAPER_ID, COMPANY_ID):
        print("⏭️  Skipping SEC EDGAR — subscription is inactive")
        return

    filing_date = date.today().isoformat()
    print(f"🔍 Scraping SEC EDGAR (8-K & D filings) for {filing_date}...")

    recent_urls = set(get_recent_article_urls(SCRAPER_ID, limit=500))

    all_items = []
    for page in range(1, MAX_PAGES + 1):
        print(f"  📄 Fetching page {page}...")
        items = fetch_page(filing_date, page)
        if not items:
            print(f"  ⛔ No results on page {page}, stopping.")
            break
        all_items.extend(items)
        time.sleep(1)

    print(f"  📰 {len(all_items)} filing(s) found across {MAX_PAGES} page(s).")

    new_items = [it for it in all_items if it["url"] not in recent_urls]
    if not new_items:
        print("⛔ No new filings since last run.")
        return

    print(f"  🆕 {len(new_items)} new filing(s) to scrape.")

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
            "date": item["date"],
            "scraper_id": SCRAPER_ID,
            "company_id": COMPANY_ID,
        }
        print(f"  ✅ {result['title'][:70]}")
        return result

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_one, item): item for item in new_items}
        for future in as_completed(futures):
            result = future.result()
            if result:
                articles.append(result)

    if not articles:
        print("⛔ No filings scraped successfully.")
        return

    inserted = insert_articles(articles)
    print(f"✅ Inserted {inserted} filings into database.")


if __name__ == "__main__":
    main()
