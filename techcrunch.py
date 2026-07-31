from bs4 import BeautifulSoup
import os
import requests
import time
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

API_URL = "https://techcrunch.com/wp-json/wp/v2/posts"
SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
SOURCE_NAME = "TECHCRUNCH"
SCRAPER_ID = 50

COMPANY_CONFIGS = [
    {
        "label": "H2 Recruit",
        "company_id": os.getenv("H2_RECRUIT_COMPANY_ID"),
    },
    {
        "label": "Intune Talent",
        "company_id": os.getenv("INTUNE_TALENT_COMPANY_ID"),
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


def fetch_posts_with_retry(page_num, max_retries=3):
    """Fetch posts via Scrappey request.get (non-browser)"""
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
            resp = requests.post(
                f"{SCRAPPEY_API_URL}?key={api_key}",
                json=payload,
                timeout=60,
            )
            # with open("output.json", "w", encoding="utf-8") as f:
            #     json.dump(resp.json(), f, indent=4, ensure_ascii=False)
            resp.raise_for_status()
            data = resp.json()
            html = data.get("solution", {}).get("innerText", "")
            if not html:
                raise RuntimeError("Empty Scrappey response")
            import json as _json
            posts = _json.loads(html)
            return posts
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {str(e)}")
                return None


def main():
    print("🔍 Fetching articles from TechCrunch API...")

    # Fetch posts once
    all_posts = []
    newest_timestamp = None

    for page_num in range(1, 2):
        print(f"📄 Fetching page {page_num}...")
        posts = fetch_posts_with_retry(page_num)

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

        if not is_subscription_active(SCRAPER_ID, company_id):
            print(f"⏭️  Skipping {label} — subscription is inactive")
            continue

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
