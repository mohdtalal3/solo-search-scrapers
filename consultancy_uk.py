import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles

load_dotenv()

NEWS_PAGE_URL = "https://www.consultancy.uk/news"
SOURCE_NAME = "CONSULTANCY_UK"
SCRAPER_ID = 16

# ----------------------------------------------------------
# Companies that receive consultancy.eu articles.
# ----------------------------------------------------------
COMPANY_CONFIGS = [
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
# Helpers
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


def parse_date(date_text):
    """Convert '27 March 2026' → '2026-03-27'."""
    try:
        return datetime.strptime(date_text.strip(), "%d %B %Y").strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return date_text.strip()


# ----------------------------------------------------------
# Fetch article links from main news page
# ----------------------------------------------------------
def get_article_links():
    html = fetch_url(NEWS_PAGE_URL)
    if not html:
        raise Exception("Failed to fetch news page.")

    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select(".news-grid .news-item-info > a"):
        href = a.get("href", "").strip()
        if not href:
            continue
        if href.startswith("http"):
            links.append(href)
        else:
            links.append("https://www.consultancy.uk" + href)

    return links


# ----------------------------------------------------------
# Scrape a single article page
# ----------------------------------------------------------
def scrape_article(url):
    html = fetch_url(url)
    if not html:
        print(f"❌ Failed to fetch {url}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.select("script, style, iframe"):
        tag.decompose()

    # Title
    h1 = soup.select_one("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""

    # Date
    date_el = soup.select_one("div.meta_news span.date")
    date_raw = date_el.get_text(strip=True) if date_el else ""
    date_iso = parse_date(date_raw) if date_raw else ""

    # Body
    content_div = soup.select_one("div.text")
    if not content_div:
        print(f"⚠️  No body content found for {url}")
        return None

    # Remove images, captions, tags, social buttons
    for tag in content_div.select("img, picture, div.main-image, div.image-subtext, div.mobileTags, div.article-social"):
        tag.decompose()

    # Unwrap links — keep text, drop href
    for a in content_div.find_all("a"):
        a.unwrap()

    paragraphs = [
        p.get_text(" ", strip=True)
        for p in content_div.find_all("p")
        if p.get_text(strip=True)
    ]
    text = "\n\n".join(paragraphs)

    return {
        "url": url,
        "date": date_iso,
        "title": title,
        "text": text,
        "lastmod": date_iso,
        "scraper_id": SCRAPER_ID,
    }


# ----------------------------------------------------------
# MAIN LOGIC
# ----------------------------------------------------------
def main():
    print("🔍 Fetching consultancy.uk news page...")
    article_links = get_article_links()
    #article_links = article_links[:15]
    if not article_links:
        print("⛔ No article links found on news page.")
        return

    print(f"📰 Found {len(article_links)} article links on page.")

    # Fetch last 32 known URLs from DB for this scraper (one query)
    known_urls = get_recent_article_urls(SCRAPER_ID, limit=32)

    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")

    new_links = [url for url in article_links if url not in known_urls]

    if not new_links:
        print("⛔ No new articles found.")
        return

    print(f"🆕 {len(new_links)} new article(s) to scrape.")

    # Scrape each new article exactly once
    scraped = []
    for url in new_links:
        print(f"  Scraping: {url}")
        result = scrape_article(url)
        if result:
            scraped.append(result)

    if not scraped:
        print("⛔ No articles scraped successfully.")
        return

    # Insert for each company (db deduplicates articles, creates per-company links)
    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        label = config["label"]

        print(f"\n{'='*60}")
        print(f"🏢 Inserting for: {label}")
        print(f"{'='*60}")

        company_articles = [dict(a, company_id=company_id) for a in scraped]
        inserted_count = insert_articles(company_articles)
        print(f"✅ Inserted {inserted_count} articles for {label}")


if __name__ == "__main__":
    main()
