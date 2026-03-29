from bs4 import BeautifulSoup
import os
import requests
import time
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles

load_dotenv()

API_URL = "https://computable.nl/wp-json/wp/v2/posts"
SOURCE_NAME = "COMPUTABLE_NL"
SCRAPER_ID = 18
COMPANY_ID = os.getenv("ERP_RECRUIT_COMPANY_ID")


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


def fetch_posts_with_retry(page_num, max_retries=3):
    """Fetch posts with retry logic"""
    url = f"{API_URL}?per_page=100&page={page_num}&orderby=date&order=desc"
    headers = {"User-Agent": "Mozilla/5.0"}

    for attempt in range(max_retries):
        try:
            time.sleep(2)  # Sleep 2 seconds before request
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            posts = response.json()
            return posts

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

    print("🔍 Fetching articles from Computable NL API...")

    # Iterate through 2 pages
    for page_num in range(1, 3):
        print(f"📄 Fetching page {page_num}...")
        posts = fetch_posts_with_retry(page_num)

        if posts is None or not posts:
            print(f"⛔ No articles found on page {page_num}.")
            break

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

        # If we found old articles, stop pagination
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
