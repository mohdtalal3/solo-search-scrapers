from bs4 import BeautifulSoup
import json as _json
import os
import requests
import time
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

API_URL = "https://www.marketingweek.com/wp-json/wp/v2/posts"
SOURCE_NAME = "MARKETING_WEEK"
SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
SCRAPER_ID = 32
COMPANY_CONFIGS = [
    {
        "label": "Headliners",
        "company_id": os.getenv("HEADLINERS_COMPANY_ID"),
    },
    {
        "label": "Time to Hire",
        "company_id": os.getenv("TIME_TO_HIRE_RECRUITMENT_LTD_COMPANY_ID"),
    },
]


def clean_html_content(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    for tag in soup.select("script, style, iframe"):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    text = " ".join(text.split())
    return text


def fetch_posts_with_retry(page_num, max_retries=3):
    params = f"per_page=100&page={page_num}&orderby=date&order=desc"
    url = f"{API_URL}?{params}"
    api_key = os.getenv("SCRAPPEY_API_KEY")
    if not api_key:
        raise RuntimeError("SCRAPPEY_API_KEY not set")

    payload = {
        "cmd": "request.get",
        "requestType": "request",
        "url": url,
        "premiumProxy": True,
        "proxyCountry": "UnitedKingdom",
        "retries": 2,
        "automaticallySolveCaptcha": True
    }

    for attempt in range(max_retries):
        try:
            time.sleep(2)
            resp = requests.post(
                f"{SCRAPPEY_API_URL}?key={api_key}",
                json=payload,
                timeout=90,
            )
            if resp.status_code == 400:
                return None  # No more pages
            resp.raise_for_status()
            data = resp.json()
            html = data.get("solution", {}).get("response", "")
            if not html:
                raise RuntimeError("Empty Scrappey response")
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
    print("🔍 Fetching articles from Marketing Week API...")

    # Collect per-company saved timestamps
    company_timestamps = {}
    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        if not is_subscription_active(SCRAPER_ID, company_id):
            print(f"⏭️  Skipping {config['label']} — subscription is inactive")
            continue
        company_timestamps[company_id] = {
            "label": config["label"],
            "saved_timestamp": get_latest_timestamp(SCRAPER_ID, company_id),
        }

    if not company_timestamps:
        print("⛔ No active companies.")
        return

    # Use the oldest saved timestamp as cutoff so we catch all new articles for all companies
    active_timestamps = [
        v["saved_timestamp"] for v in company_timestamps.values()
        if v["saved_timestamp"]
    ]
    earliest_cutoff = min(active_timestamps) if active_timestamps else None

    all_posts = []
    newest_timestamp = None

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
            all_posts.append(post)

        if earliest_cutoff and any(p["date_gmt"] <= earliest_cutoff for p in posts):
            break

    if not all_posts:
        print("⛔ No articles found.")
        return

    # Process each company
    for company_id, info in company_timestamps.items():
        label = info["label"]
        saved_timestamp = info["saved_timestamp"]

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        if saved_timestamp is None:
            print("🟢 First run detected — NOT saving any articles.")
            if newest_timestamp:
                print("Saving latest timestamp:", newest_timestamp)
                update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
            continue

        company_articles = []
        for post in all_posts:
            timestamp = post["date_gmt"]
            if timestamp <= saved_timestamp:
                continue

            title = post["title"]["rendered"]
            html_content = post["content"]["rendered"]
            text = clean_html_content(html_content)

            company_articles.append({
                "url": post["link"],
                "date": timestamp,
                "title": title,
                "text": text,
                "lastmod": timestamp,
                "company_id": company_id,
                "scraper_id": SCRAPER_ID,
            })

        if not company_articles:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(company_articles)} new articles.")
        inserted_count = insert_articles(company_articles)
        print(f"✅ Inserted {inserted_count} articles into database")

        company_newest = max(p["date_gmt"] for p in all_posts if p["date_gmt"] > saved_timestamp)
        if company_newest:
            update_latest_timestamp(SCRAPER_ID, company_id, company_newest)
            print("🕒 New latest timestamp saved:", company_newest)


if __name__ == "__main__":
    main()
