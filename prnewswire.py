import os
import time

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles

SITEMAP_URL = "https://www.prnewswire.co.uk/sitemap-news.xml?page=1"
SOURCE_NAME = "PR_NEWSWIRE_UK"
SCRAPER_ID = 14
COMPANY_ID = os.getenv("ARDEN_EXEC_COMPANY_ID")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
}

load_dotenv()


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
        "company_id": COMPANY_ID,
        "scraper_id": SCRAPER_ID,
    }


# ----------------------------------------------------------
# MAIN LOGIC
# ----------------------------------------------------------
def main():
    saved_timestamp = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)

    print("🔍 Fetching PR Newswire UK sitemap (page 1)...")
    article_entries = get_articles_from_sitemap()

    if not article_entries:
        print("⛔ No articles found in sitemap.")
        return

    # Sort newest first
    article_entries.sort(key=lambda x: x["date"], reverse=True)
    newest_timestamp = article_entries[0]["date"]

    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT scraping any articles.")
        print("Saving latest timestamp:", newest_timestamp)
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        return

    # ----------------------------
    # SUBSEQUENT RUNS — scrape new
    # ----------------------------
    print("Previously saved timestamp:", saved_timestamp)

    new_articles = [a for a in article_entries if a["date"] > saved_timestamp]

    if not new_articles:
        print("⛔ No new articles found.")
        return

    print(f"🆕 Found {len(new_articles)} new articles.")

    scraped_articles = []
    for entry in new_articles:
        print("Scraping:", entry["url"])
        scraped = scrape_article(entry["url"], entry["date"], entry["title"])
        if scraped:
            scraped_articles.append(scraped)

    if scraped_articles:
        inserted_count = insert_articles(scraped_articles)
        print(f"✅ Inserted {inserted_count} articles into database")

    update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
