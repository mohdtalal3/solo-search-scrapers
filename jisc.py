import os
import json
import time
from bs4 import BeautifulSoup
import requests as std_requests
from dotenv import load_dotenv
from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

LISTING_URL = "https://www.jisc.ac.uk/intelligence-ideas-insights"
BASE_URL = "https://www.jisc.ac.uk"
SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
SOURCE_NAME = "JISC"
SCRAPER_ID = 46

COMPANY_CONFIGS = [
    {
        "label": "Connected IT",
        "company_id": os.getenv("CONNECTED_IT_COMPANY_ID"),
    },
]

def fetch_next_data(url, max_retries=3):
    """Fetch a page via Scrappey and extract the __NEXT_DATA__ JSON."""
    api_key = os.getenv("SCRAPPEY_API_KEY")
    if not api_key:
        raise RuntimeError("SCRAPPEY_API_KEY not set")

    payload = {
        "cmd": "request.get",
        "requestType": "request",
        "url": url,
        "premiumProxy": True,
        #"proxyCountry": "UnitedKingdom",
        "retries": 1,
        #"automaticallySolveCaptcha": True
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
            solution = data.get("solution", {})
            html = solution.get("response", "")
            if not html:
                raise RuntimeError("Empty Scrappey response")

            soup = BeautifulSoup(html, "html.parser")
            script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if not script_tag:
                raise RuntimeError("__NEXT_DATA__ script tag not found")
            return json.loads(script_tag.string)
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


def clean_html(html):
    """Strip HTML tags and return clean text."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    return " ".join(text.split())


def scrape_article(url):
    """Fetch an article page and extract fields from __NEXT_DATA__."""
    data = fetch_next_data(url)
    if not data:
        return None

    page_data = data.get("props", {}).get("pageProps", {}).get("pageData", {})

    title = page_data.get("title", "")
    date = page_data.get("meta", {}).get("first_published_at", "")

    # Extract text from page_body content blocks
    parts = []
    for block in page_data.get("page_body", []):
        if block.get("type") == "content_block":
            parts.append(clean_html(block.get("value", "")))
        elif block.get("type") == "pull_quote":
            quote_html = block.get("value", {}).get("quote", "")
            if quote_html:
                parts.append(clean_html(quote_html))

    text = "\n\n".join(p for p in parts if p)

    return {
        "url": url,
        "scraper_id": SCRAPER_ID,
        "date": date,
        "title": title,
        "text": text,
    }


def get_blogs_from_listing():
    """Fetch the listing page and return list of blog entries with url + first_published_at."""
    data = fetch_next_data(LISTING_URL)
    if not data:
        raise RuntimeError("Failed to fetch listing page")

    page_data = data.get("props", {}).get("pageProps", {}).get("pageData", {})
    blogs = page_data.get("blogs", [])

    entries = []
    for blog in blogs:
        relative_url = blog.get("url", "")
        published_at = blog.get("first_published_at", "")
        if relative_url and published_at:
            entries.append({
                "url": BASE_URL + relative_url,
                "first_published_at": published_at,
            })

    return entries


def main():
    print("🔍 Fetching blog listing from Jisc...")
    blog_entries = get_blogs_from_listing()

    if not blog_entries:
        print("⛔ No blog entries found on listing page.")
        return

    # Sort newest first
    blog_entries.sort(key=lambda x: x["first_published_at"], reverse=True)
    newest_timestamp = blog_entries[0]["first_published_at"]

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

        new_entries = [e for e in blog_entries if e["first_published_at"] > saved_timestamp]

        if not new_entries:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_entries)} new articles.")

        scraped_articles = []
        for entry in new_entries:
            print("Scraping:", entry["url"])
            article = scrape_article(entry["url"])
            if article:
                article["lastmod"] = entry["first_published_at"]
                scraped_articles.append(article)

        if scraped_articles:
            company_articles = [dict(a, company_id=company_id) for a in scraped_articles]
            inserted_count = insert_articles(company_articles)
            print(f"✅ Inserted {inserted_count} articles for {label}")

        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
