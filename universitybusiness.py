import os
import requests
from bs4 import BeautifulSoup
import time
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

API_URL = "https://universitybusiness.co.uk/wp-json/wp/v2/posts"
SOURCE_NAME = "UNIVERSITY_BUSINESS"
SCRAPER_ID = 45

COMPANY_CONFIGS = [
    {
        "label": "Connected IT",
        "company_id": os.getenv("CONNECTED_IT_COMPANY_ID"),
    },
]


def clean_html_content(html_content):
    soup = BeautifulSoup(html_content, "html.parser")

    for tag in soup.select("script, style, iframe"):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    text = " ".join(text.split())
    return text


def fetch_posts_with_retry(session, params, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(2)
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

        print("🔍 Fetching articles from University Business API...")

        all_articles = []
        newest_timestamp = None

        for page_num in range(1, 3):
            params = {
                "per_page": 100,
                "page": page_num,
                "orderby": "date",
                "order": "desc",
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

                if saved_timestamp and timestamp <= saved_timestamp:
                    break

                title = post["title"]["rendered"]
                html_content = post["content"]["rendered"]
                text = clean_html_content(html_content)

                article = {
                    "url": post["link"],
                    "scraper_id": SCRAPER_ID,
                    "date": timestamp,
                    "title": title,
                    "text": text,
                    "lastmod": timestamp,
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
                update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        # ----------------------------
        # SUBSEQUENT RUNS — save new
        # ----------------------------
        print("Previously saved timestamp:", saved_timestamp)

        if not all_articles:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(all_articles)} new articles.")

        company_articles = [dict(a, company_id=company_id) for a in all_articles]
        inserted_count = insert_articles(company_articles)
        print(f"✅ Inserted {inserted_count} articles for {label}")

        if newest_timestamp:
            update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
