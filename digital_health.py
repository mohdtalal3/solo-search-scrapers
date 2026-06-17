import os
import json
import time
from bs4 import BeautifulSoup
import requests as std_requests
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

API_URL = "https://www.digitalhealth.net/wp-json/wp/v2/posts"
SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
SOURCE_NAME = "DIGITAL_HEALTH"
SCRAPER_ID = 3

COMPANY_CONFIGS = [
    {
        "label": "Solo Search",
        "company_id": os.getenv("SOLO_SEARCH_COMPANY_ID"),
    },
    # {
    #     "label": "Connected IT",
    #     "company_id": os.getenv("CONNECTED_IT_COMPANY_ID"),
    # },
]


def clean_html_content(html_content):
    """Convert HTML content to clean text."""
    soup = BeautifulSoup(html_content, "html.parser")
    for tag in soup.select("script, style, iframe"):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return " ".join(text.split())


def fetch_posts_with_retry(page_num, max_retries=3):
    """Fetch posts via Scrappey request.get (non-browser)."""
    url = f"{API_URL}?per_page=100&page={page_num}&orderby=date&order=desc"
    api_key = os.getenv("SCRAPPEY_API_KEY")
    if not api_key:
        raise RuntimeError("SCRAPPEY_API_KEY not set")

    payload = {
        "cmd": "request.get",
        "url": url,
        "premiumProxy": True,
        "proxyCountry": "UnitedKingdom",
        "retries": 1,
        "automaticallySolveCaptcha": True,
    }

    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = std_requests.post(
                f"{SCRAPPEY_API_URL}?key={api_key}",
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            html = data.get("solution", {}).get("innerText", "")
            if not html:
                raise RuntimeError("Empty Scrappey response")
            posts = json.loads(html)
            return posts
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {str(e)}")
                return None


def main():
    all_posts = []
    newest_timestamp = None

    print("🔍 Fetching articles from Digital Health API...")

    for page_num in range(1, 2):
        print(f"📄 Fetching page {page_num}...")
        posts = fetch_posts_with_retry(page_num)

        if not posts:
            print(f"⛔ No articles found on page {page_num}.")
            break

        for post in posts:
            timestamp = post["date_gmt"]
            if newest_timestamp is None:
                newest_timestamp = timestamp

            title = post["title"]["rendered"]
            html_content = post["content"]["rendered"]
            text = clean_html_content(html_content)

            all_posts.append({
                "url": post["link"],
                "date": timestamp,
                "title": title,
                "text": text,
                "lastmod": timestamp,
                "scraper_id": SCRAPER_ID,
            })

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
            print("🟢 First run detected — NOT saving any articles.")
            if newest_timestamp:
                print("Saving latest timestamp:", newest_timestamp)
                update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        print("Previously saved timestamp:", saved_timestamp)

        new_articles = [a for a in all_posts if a["lastmod"] > saved_timestamp]

        if not new_articles:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_articles)} new articles.")

        company_articles = [dict(a, company_id=company_id) for a in new_articles]
        inserted_count = insert_articles(company_articles)
        print(f"✅ Inserted {inserted_count} articles for {label}")

        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
