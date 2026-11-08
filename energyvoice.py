import os
import time
from urllib.parse import urlencode

from curl_cffi import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles

API_URL = "https://www.energyvoice.com/wp-json/wp/v2/posts"
SOURCE_NAME = "ENERGY_VOICE"
SCRAPER_ID = 13
COMPANY_ID = os.getenv("ARDEN_EXEC_COMPANY_ID")

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

load_dotenv()


def clean_html_content(html_content):
    """Convert HTML content to clean text"""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove unwanted elements
    for tag in soup.select("script, style, iframe"):
        tag.decompose()

    # Get text and clean up whitespace
    text = soup.get_text(" ", strip=True)
    text = " ".join(text.split())
    return text


def build_target_url(params):
    """Build the WordPress API URL with query parameters."""
    return f"{API_URL}?{urlencode(params)}"


def fetch_posts_with_retry(params, max_retries=3):
    """Fetch posts via curl_cffi (Chrome impersonation + proxy)."""
    target_url = build_target_url(params)

    for attempt in range(max_retries):
        try:
            time.sleep(2)  # Sleep 2 seconds before request
            response = requests.get(
                target_url,
                headers=HEADERS,
                proxies=PROXIES,
                impersonate="chrome131",
                timeout=60,
            )

            if response.status_code == 400:
                return None  # No more pages

            response.raise_for_status()
            return response.json()

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

    print("🔍 Fetching articles from Energy Voice API...")

    # Fetch first page only
    params = {
        "per_page": 100,
        "page": 1,
        "orderby": "date",
        "order": "desc"
    }

    posts = fetch_posts_with_retry(params)

    if posts is None or not posts:
        print("⛔ No articles found.")
        return

    for post in posts:
        timestamp = post["date_gmt"]

        # Set newest timestamp from first article
        if newest_timestamp is None:
            newest_timestamp = timestamp

        # Stop if we've reached articles older than saved timestamp
        if saved_timestamp and timestamp <= saved_timestamp:
            break

        # Extract and clean content
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
            "scraper_id": SCRAPER_ID
        }

        all_articles.append(article)
        print(f"Fetched: {title[:60]}...")



   # input("Press Enter to continue...")  # Pause before finalizing
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

    # Insert articles into database
    if all_articles:
        inserted_count = insert_articles(all_articles)
        print(f"✅ Inserted {inserted_count} articles into database")

    # Update timestamp
    if newest_timestamp:
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
