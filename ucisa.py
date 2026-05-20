import os
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

SITEMAP_URL = "https://www.ucisa.ac.uk/sitemap.xml"
BASE_URL = "https://www.ucisa.ac.uk"
NEWS_PATH = "/news-and-blogs/"
SOURCE_NAME = "UCISA"
SCRAPER_ID = 47

COMPANY_CONFIGS = [
    {
        "label": "Connected IT",
        "company_id": os.getenv("CONNECTED_IT_COMPANY_ID"),
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
}


def fetch_url(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


def get_articles_from_sitemap():
    """Fetch sitemap and return news-and-blogs entries with url + lastmod."""
    html = fetch_url(SITEMAP_URL)
    if not html:
        raise RuntimeError("Failed to fetch sitemap")

    soup = BeautifulSoup(html, "lxml")
    entries = []

    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        lastmod = url_tag.find("lastmod")
        if not loc:
            continue

        url = loc.text.strip()
        # Only include news-and-blogs articles (not the listing page itself)
        path = url.replace(BASE_URL, "")
        if not path.startswith(NEWS_PATH) or path == NEWS_PATH:
            continue

        timestamp = lastmod.text.strip() if lastmod else ""
        entries.append({"url": url, "lastmod": timestamp})

    return entries


def scrape_article(url):
    """Fetch an article page and extract title, date, and text."""
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    content_div = soup.select_one("section.container div.aimUCISA")
    if not content_div:
        return None

    # Title and date are in the h3: "21 April 2026 - Neurodiversity FAQs"
    h3 = content_div.find("h3")
    title = ""
    date = ""
    if h3:
        raw = h3.get_text(strip=True)
        if " - " in raw:
            raw_date, title = raw.split(" - ", 1)
            try:
                date = datetime.strptime(raw_date.strip(), "%d %B %Y").strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                date = raw_date.strip()
            title = title.strip()
        else:
            title = raw

    # Remove the h3 before extracting body text
    if h3:
        h3.decompose()

    # Remove noisy elements
    for tag in content_div.select("script, style, iframe, img"):
        tag.decompose()

    text = content_div.get_text(" ", strip=True)
    text = " ".join(text.split())

    return {
        "url": url,
        "scraper_id": SCRAPER_ID,
        "date": date,
        "title": title,
        "text": text,
    }


def main():
    print("🔍 Fetching sitemap from UCISA...")
    article_entries = get_articles_from_sitemap()

    if not article_entries:
        print("⛔ No news-and-blogs entries found in sitemap.")
        return

    # Sort newest first by lastmod
    article_entries.sort(key=lambda x: x["lastmod"], reverse=True)
    newest_timestamp = article_entries[0]["lastmod"]

    print(f"Found {len(article_entries)} news-and-blogs URLs in sitemap.")

    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        label = config["label"]

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        if not is_subscription_active(SCRAPER_ID, company_id):
            print(f"⏭️  Skipping {label} — subscription is inactive")
            continue

        saved_timestamp = get_latest_timestamp(SCRAPER_ID, company_id)

        # ----------------------------
        # FIRST RUN — NO SCRAPING
        # ----------------------------
        if saved_timestamp is None:
            print("🟢 First run detected — NOT scraping any articles.")
            print("Saving latest timestamp:", newest_timestamp)
            update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        # ----------------------------
        # SUBSEQUENT RUNS — scrape new
        # ----------------------------
        print("Previously saved timestamp:", saved_timestamp)

        new_entries = [e for e in article_entries if e["lastmod"] > saved_timestamp]

        if not new_entries:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_entries)} new articles.")

        scraped_articles = []
        for entry in new_entries:
            print("Scraping:", entry["url"])
            article = scrape_article(entry["url"])
            if article:
                article["lastmod"] = entry["lastmod"]
                scraped_articles.append(article)

        if scraped_articles:
            company_articles = [dict(a, company_id=company_id) for a in scraped_articles]
            inserted_count = insert_articles(company_articles)
            print(f"✅ Inserted {inserted_count} articles for {label}")

        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
