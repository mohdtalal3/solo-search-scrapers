import json
import os
import time
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

API_URL = "https://www.eu-startups.com/wp-json/wp/v2/posts"
SOURCE_NAME = "EU_STARTUPS"
SCRAPER_ID = 4
SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
SCRAPPEY_PROXY_COUNTRY = "UnitedKingdom"

COMPANY_CONFIGS = [
    {
        "label": "Solo Search",
        "company_id": os.getenv("SOLO_SEARCH_COMPANY_ID"),
    },
    {
        "label": "H2 Recruit",
        "company_id": os.getenv("H2_RECRUIT_COMPANY_ID"),
    },
]

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


def extract_posts_from_scrappey(scrappey_response):
    """Parse JSON returned through Scrappey's browser response."""
    solution = scrappey_response.get("solution", {})
    inner_text = (solution.get("innerText") or "").strip()

    if inner_text:
        return json.loads(inner_text)

    response_html = solution.get("response") or ""
    if response_html:
        soup = BeautifulSoup(response_html, "html.parser")
        pre_tag = soup.find("pre")
        if pre_tag and pre_tag.get_text(strip=True):
            return json.loads(pre_tag.get_text(strip=True))

    raise ValueError("Scrappey returned no parseable JSON payload")


def fetch_posts_with_retry(params, max_retries=3):
    """Fetch posts through Scrappey with premium US proxy and browser anti-bot handling."""
    scrappey_api_key = os.getenv("SCRAPPEY_API_KEY")
    if not scrappey_api_key:
        raise RuntimeError("Please set SCRAPPEY_API_KEY in your environment")

    target_url = build_target_url(params)
    payload = {
        "cmd": "request.get",
        "url": target_url,
        "premiumProxy": True,
        "proxyCountry": SCRAPPEY_PROXY_COUNTRY,
        "retries": 1,
        "browserActions": [
            {
                "type": "wait_for_load_state",
                "waitForLoadState": "networkidle"
            },
            {
                "type": "wait",
                "wait": 1500,
                "when": "after_captcha"
            }
        ]
    }

    for attempt in range(max_retries):
        try:
            time.sleep(2)  # Sleep 2 seconds before request
            response = requests.post(
                f"{SCRAPPEY_API_URL}?key={scrappey_api_key}",
                json=payload,
                timeout=90,
            )
            response.raise_for_status()

            scrappey_response = response.json()
            solution = scrappey_response.get("solution", {})
            status_code = solution.get("statusCode")

            if status_code == 400:
                return None  # No more pages

            if scrappey_response.get("data") == "error" or not solution.get("verified", False):
                error_message = scrappey_response.get("error", "Unknown Scrappey error")
                raise RuntimeError(error_message)

            return extract_posts_from_scrappey(scrappey_response)

        except (requests.RequestException, ValueError, RuntimeError) as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {str(e)}")
                return None


def main():
    print("🔍 Fetching articles from EU-Startups API...")

    params = {
        "categories": 1282,  # Funding category
        "per_page": 100,
        "page": 1,
        "orderby": "date",
        "order": "desc"
    }

    posts = fetch_posts_with_retry(params)

    if not posts:
        print("⛔ No articles found.")
        return

    all_posts = []
    newest_timestamp = None

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
        print(f"Fetched: {title[:60]}...")

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