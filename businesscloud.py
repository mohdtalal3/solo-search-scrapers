import os
import requests
from bs4 import BeautifulSoup
import time
from db import get_latest_timestamp, update_latest_timestamp, insert_articles

API_URL = "https://businesscloud.co.uk/wp-json/wp/v2/posts"
SOURCE_NAME = "BUSINESS_CLOUD"
SCRAPER_ID = 1
COMPANY_CONFIGS = [
    {
        "label": "Solo Search",
        "company_id": os.getenv("SOLO_SEARCH_COMPANY_ID"),
    },
    {
        "label": "Solis Recruitment",
        "company_id": os.getenv("SOLIS_RECRUITMENT_COMPANY_ID"),
    },
]

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


def fetch_posts_with_retry(session, params, max_retries=3):
    """Fetch posts with retry logic"""
    for attempt in range(max_retries):
        try:
            time.sleep(2)  # Sleep 2 seconds before request
            r = session.get(API_URL, params=params, timeout=30)
            
            if r.status_code == 400:
                return None  # No more pages
            
            r.raise_for_status()
            return r.json()
            
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {str(e)}")
                return None


def main():
    session = requests.Session()

    all_posts = []
    newest_timestamp = None

    print("🔍 Fetching articles from BusinessCloud API...")

    # Iterate through 2 pages
    for page_num in range(1, 3):
        params = {
            "per_page": 100,
            "page": page_num,
            "orderby": "date",
            "order": "desc"
        }

        print(f"📄 Fetching page {page_num}...")
        posts = fetch_posts_with_retry(session, params)

        if posts is None or not posts:
            print(f"⛔ No articles found on page {page_num}.")
            break

        for post in posts:
            timestamp = post["date_gmt"]
            if newest_timestamp is None:
                newest_timestamp = timestamp
            all_posts.append(post)

    if not all_posts:
        print("⛔ No articles fetched.")
        return

    print(f"📊 Fetched {len(all_posts)} posts total.")

    # Process for each company
    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        label = config["label"]

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        saved_timestamp = get_latest_timestamp(SCRAPER_ID, company_id)

        all_articles = []
        for post in all_posts:
            timestamp = post["date_gmt"]

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
                "company_id": company_id,
                "scraper_id": SCRAPER_ID,
            }

            all_articles.append(article)
            print(f"  Fetched: {title[:60]}...")

        # FIRST RUN — NO SCRAPING
        if saved_timestamp is None:
            print("🟢 First run detected — NOT saving any articles.")
            if newest_timestamp:
                print("Saving latest timestamp:", newest_timestamp)
                update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        print("Previously saved timestamp:", saved_timestamp)

        if not all_articles:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(all_articles)} new articles.")

        inserted_count = insert_articles(all_articles)
        print(f"✅ Inserted {inserted_count} articles into database")

        if newest_timestamp:
            update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
