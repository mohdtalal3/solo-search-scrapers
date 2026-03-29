import os
import time

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles

load_dotenv()

SITEMAP_URL = "https://www.prnewswire.co.uk/sitemap-news.xml?page=1"
SOURCE_NAME = "PR_NEWSWIRE_UK"
SCRAPER_ID = 14

# ----------------------------------------------------------
# All companies that receive PR Newswire articles.
# No per-company filters — every company gets all articles.
# ----------------------------------------------------------
COMPANY_CONFIGS = [
    {
        "label": "Arden Executive",
        "company_id": os.getenv("ARDEN_EXEC_COMPANY_ID"),
    },
    {
        "label": "ERP Recruit",
        "company_id": os.getenv("ERP_RECRUIT_COMPANY_ID"),
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
}


# ----------------------------------------------------------
# Fetch a URL with simple requests and retry logic
# ----------------------------------------------------------
def fetch_url(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {str(e)}")
                return None


# ----------------------------------------------------------
# Parse sitemap and return list of {url, date, title}
# ----------------------------------------------------------
def get_articles_from_sitemap():
    html = fetch_url(SITEMAP_URL)
    if not html:
        raise Exception("Failed to fetch sitemap via Scrappey.")

    soup = BeautifulSoup(html, "xml")

    articles = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        pub_date = url_tag.find("news:publication_date")
        news_title = url_tag.find("news:title")

        if not loc or not pub_date:
            continue

        articles.append({
            "url": loc.get_text(strip=True),
            "date": pub_date.get_text(strip=True),
            "title": news_title.get_text(strip=True) if news_title else "",
        })

    return articles


# ----------------------------------------------------------
# Scrape a single article page
# ----------------------------------------------------------
def scrape_article(url, date, title):
    html = fetch_url(url)
    if not html:
        print(f"❌ Failed to fetch {url}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Remove scripts and styles
    for tag in soup.select("script, style, iframe"):
        tag.decompose()

    # Title from page (fall back to sitemap title)
    h1 = soup.select_one("div.detail-headline h1")
    page_title = h1.get_text(" ", strip=True) if h1 else title

    # Article body — paragraphs inside the release body column
    body_col = soup.select_one("section.release-body div.col-lg-10")
    if not body_col:
        # Wider fallback
        body_col = soup.select_one("section.release-body")

    if not body_col:
        print(f"⚠️  Could not find body content for {url}")
        return None

    paragraphs = [
        p.get_text(" ", strip=True)
        for p in body_col.find_all("p")
        if p.get_text(strip=True)
    ]
    text = "\n\n".join(paragraphs)

    return {
        "url": url,
        "date": date,
        "title": page_title,
        "text": text,
        "lastmod": date,
        "scraper_id": SCRAPER_ID,
    }


# ----------------------------------------------------------
# MAIN LOGIC
# ----------------------------------------------------------
def main():
    # Collect saved timestamps for every company
    company_timestamps = {
        config["company_id"]: get_latest_timestamp(SCRAPER_ID, config["company_id"])
        for config in COMPANY_CONFIGS
    }

    print("🔍 Fetching PR Newswire UK sitemap (page 1)...")
    article_entries = get_articles_from_sitemap()

    if not article_entries:
        print("⛔ No articles found in sitemap.")
        return

    # Sort newest first
    article_entries.sort(key=lambda x: x["date"], reverse=True)
    newest_timestamp = article_entries[0]["date"]

    # ----------------------------
    # Determine which URLs need scraping (union across all non-first-run companies)
    # ----------------------------
    urls_to_scrape = set()
    for config in COMPANY_CONFIGS:
        ts = company_timestamps[config["company_id"]]
        if ts is not None:
            for entry in article_entries:
                if entry["date"] > ts:
                    urls_to_scrape.add(entry["url"])

    # Scrape each article exactly once and cache the result
    scraped_cache = {}
    if urls_to_scrape:
        print(f"🔎 Scraping {len(urls_to_scrape)} unique article(s)...")
        for entry in article_entries:
            if entry["url"] in urls_to_scrape:
                print("Scraping:", entry["url"])
                result = scrape_article(entry["url"], entry["date"], entry["title"])
                if result:
                    scraped_cache[entry["url"]] = result

    # ----------------------------
    # Process each company independently
    # ----------------------------
    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        label = config["label"]
        ts = company_timestamps[company_id]

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        # FIRST RUN — just save timestamp, no articles
        if ts is None:
            print("🟢 First run detected — NOT scraping any articles.")
            print("Saving latest timestamp:", newest_timestamp)
            update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        print("Previously saved timestamp:", ts)

        new_entries = [e for e in article_entries if e["date"] > ts]

        if not new_entries:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_entries)} new article(s).")

        # Build article list for this company, reusing cached scrape results
        company_articles = []
        for entry in new_entries:
            cached = scraped_cache.get(entry["url"])
            if cached:
                article = dict(cached)
                article["company_id"] = company_id
                company_articles.append(article)

        if company_articles:
            inserted_count = insert_articles(company_articles)
            print(f"✅ Inserted {inserted_count} articles for {label}")

        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        print(f"🕒 New latest timestamp saved for {label}: {newest_timestamp}")


if __name__ == "__main__":
    main()
