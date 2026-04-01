import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles

load_dotenv()

SOURCE_NAME = "CAPGEMINI"
SCRAPER_ID = 19
COMPANY_ID = os.getenv("ERP_RECRUIT_COMPANY_ID")

REGIONS = [
    {"label": "UK",          "cc": "gb-en"},
    {"label": "Belgium",     "cc": "be-en"},
    {"label": "Netherlands", "cc": "nl-nl"},
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "x-requested-with": "XMLHttpRequest",
}


def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def parse_date(date_str):
    """Convert 'Mar 16, 2026' to ISO format '2026-03-16T00:00:00Z'."""
    try:
        dt = datetime.strptime(date_str.strip(), "%b %d, %Y")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return date_str.strip()


def fetch_region_entries(cc):
    """Fetch all press release listings for a region via the API (current + previous year)."""
    current_year = datetime.now().year
    entries = []
    seen_ids = set()

    url = (
        f"https://www.capgemini.com/{cc}/wp-json/macs/v1/"
        f"press-release_search_results?filteryear={current_year}"
    )
    try:
        time.sleep(1)
        resp = requests.get(
            url,
            headers={**HEADERS, "referer": f"https://www.capgemini.com/{cc}/news/press-releases/"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    ❌ API error: {e}")
        return []

    for item in data.get("results", []):
        if item["ID"] not in seen_ids:
            seen_ids.add(item["ID"])
            entries.append({
                "url": item["url"],
                "title": item["title"],
                "date": parse_date(item["date"]),
            })

    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries


def scrape_article_body(url, max_retries=3):
    """Scrape body text from a Capgemini article page."""
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            for tag in soup.select("script, style, iframe"):
                tag.decompose()

            body_div = soup.select_one("div.article-text")
            if not body_div or not body_div.get_text(strip=True):
                body_div = soup.select_one("section.wp-block-cg-blocks-group")

            if not body_div:
                return ""

            paragraphs = [
                p.get_text(" ", strip=True)
                for p in body_div.find_all("p")
                if p.get_text(strip=True)
            ]
            return "\n\n".join(paragraphs)

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"    ⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"    ❌ Failed to scrape {url}: {e}")
                return ""


def fetch_region(label, cc, known_urls, seen_slugs):
    print(f"\n🌍 Region: {label}")
    entries = fetch_region_entries(cc)
    print(f"  🔍 API returned {len(entries)} articles.")

    new_entries = []
    for entry in entries:
        slug = url_slug(entry["url"])
        if entry["url"] in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate across regions): {slug}")
            continue
        new_entries.append(entry)
        seen_slugs.add(slug)

    if not new_entries:
        print(f"  ⛔ No new articles.")
        return []

    print(f"  🆕 {len(new_entries)} new article(s) to scrape.")

    articles = []
    for entry in new_entries:
        print(f"  Scraping: {entry['url']}")
        body = scrape_article_body(entry["url"])
        articles.append({
            "url": entry["url"],
            "date": entry["date"],
            "title": entry["title"],
            "text": body,
            "company_id": COMPANY_ID,
            "scraper_id": SCRAPER_ID,
        })
        print(f"  ✅ {entry['title'][:60]}...")

    return articles


def main():
    print("🔍 Fetching Capgemini press releases (all regions)...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")

    seen_slugs = {url_slug(u) for u in known_urls}

    all_articles = []

    for region in REGIONS:
        region_articles = fetch_region(region["label"], region["cc"], known_urls, seen_slugs)
        for a in region_articles:
            known_urls.add(a["url"])
        all_articles.extend(region_articles)

    if not all_articles:
        print("\n⛔ No new articles found across all regions.")
        return

    print(f"\n🆕 Found {len(all_articles)} new article(s) in total.")
    inserted_count = insert_articles(all_articles)
    print(f"✅ Inserted {inserted_count} articles into database")


if __name__ == "__main__":
    main()
