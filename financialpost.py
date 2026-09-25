import os
import time

from curl_cffi import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles

load_dotenv()

API_URL = "https://financialpost.com/wp-json/wp/v2/posts"
CATEGORY_ID = "699736248"  # Finance (fp-finance)
SOURCE_NAME = "FINANCIAL_POST"
SCRAPER_ID = 89
COMPANY_ID = os.getenv("TALENT_TO_HIRE")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None


def clean_html_content(html_content):
    """Convert HTML content to clean text"""
    soup = BeautifulSoup(html_content, "html.parser")

    for tag in soup.select("script, style, iframe"):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    text = " ".join(text.split())
    return text


def fetch_posts_with_retry(page_num, max_retries=3):
    """Fetch posts from the Financial Post WP API"""
    url = (
        f"{API_URL}?categories={CATEGORY_ID}"
        f"&per_page=100&page={page_num}&orderby=date&order=desc"
    )

    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = requests.get(
                url,
                headers=HEADERS,
                proxies=PROXIES,
                impersonate="chrome131",
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {str(e)}")
                return None


def main():
    saved_timestamp = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)

    all_articles = []
    newest_timestamp = None

    print("🔍 Fetching articles from Financial Post API (Finance)...")

    for page_num in range(1, 3):
        print(f"📄 Fetching page {page_num}...")
        posts = fetch_posts_with_retry(page_num)

        if posts is None or not posts:
            print(f"⛔ No articles found on page {page_num}.")
            break

        for post in posts:
            timestamp = post["date_gmt"]

            if newest_timestamp is None:
                newest_timestamp = timestamp

            if saved_timestamp and timestamp <= saved_timestamp:
                break

            title = post["title"]["rendered"]
            html_content = post["content"]["rendered"]
            text = clean_html_content(html_content)

            article = {
                "url": post["link"],
                "date": timestamp,
                "title": title,
                "text": text,
                "lastmod": timestamp,
                "company_id": COMPANY_ID,
                "scraper_id": SCRAPER_ID,
            }

            all_articles.append(article)
            print(f"Fetched: {title[:60]}...")

        if saved_timestamp and any(post["date_gmt"] <= saved_timestamp for post in posts):
            break

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

    if not all_articles:
        print("⛔ No new articles found.")
        return

    print(f"🆕 Found {len(all_articles)} new articles.")

    inserted_count = insert_articles(all_articles)
    print(f"✅ Inserted {inserted_count} articles into database")

    if newest_timestamp:
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
