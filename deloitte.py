import os
import time

import requests
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles

load_dotenv()

SOURCE_NAME = "DELOITTE"
SCRAPER_ID = 21
COMPANY_ID = os.getenv("ERP_RECRUIT_COMPANY_ID")

API_URL = "https://www.deloitte.com/modern-prod-english/_search"
BASE_URL = "https://www.deloitte.com"
PAGE_SIZE = 20

REGIONS = [
    {"label": "UK", "cc": "uk"},
    {"label": "NL", "cc": "nl"},
    {"label": "BE", "cc": "be"},
    {"label": "IE", "cc": "ie"},
    {"label": "LU", "cc": "lu"},
]

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "d-target": "elastic",
    "origin": BASE_URL,
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


def url_slug(url):
    """Extract the final path segment (without .html) for cross-region dedup."""
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    if slug.endswith(".html"):
        slug = slug[:-5]
    return slug


def fetch_region(label, cc, known_urls, seen_slugs):
    """Fetch press-room articles for a region via the Elasticsearch API."""
    print(f"\n🌍 Region: {label}")

    payload = {
        "from": 0,
        "size": PAGE_SIZE,
        "sort": [{"date-published": "desc"}],
        "query": {
            "bool": {
                "must": [
                    {"match": {"site-name.raw": cc}},
                    {"match": {"language.raw": "en"}},
                    {"terms": {"content-type.keyword": ["News"]}},
                    {"prefix": {"url.keyword": f"/{cc}/en/about/press-room"}},
                ],
                "must_not": [
                    {"terms": {"page-type.keyword": [
                        "premium-event-speaker", "premium-event-filter",
                        "premium-event-info", "premium-event-session", "dep-profile",
                    ]}},
                    {"match": {"hidefrominternalsearchandfilters": "true"}},
                ],
            }
        },
    }

    try:
        time.sleep(1)
        resp = requests.post(
            API_URL,
            json=payload,
            headers={**HEADERS, "referer": f"{BASE_URL}/{cc}/en/about/press-room.html"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ❌ API request failed: {e}")
        return []

    hits = data.get("hits", {}).get("hits", [])
    print(f"  🔍 API returned {len(hits)} articles.")

    articles = []
    for hit in hits:
        src = hit.get("_source", {})
        rel_url = src.get("url", "")
        if not rel_url:
            continue

        full_url = BASE_URL + rel_url
        slug = url_slug(rel_url)

        if full_url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate across regions): {slug}")
            continue

        seen_slugs.add(slug)

        title = src.get("title", "").strip()
        body = (src.get("body") or "").strip()
        date_published = src.get("date-published", "")
        # Strip timezone suffix to get plain ISO datetime
        date_published = date_published[:19] if date_published else ""

        if not title:
            continue

        articles.append({
            "url": full_url,
            "date": date_published,
            "title": title,
            "text": body,
            "company_id": COMPANY_ID,
            "scraper_id": SCRAPER_ID,
        })
        print(f"  ✅ {title[:60]}...")

    return articles


def main():
    print("🔍 Fetching Deloitte press room articles (all regions)...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")

    seen_slugs = {url_slug(u) for u in known_urls}

    all_articles = []

    for region in REGIONS:
        region_articles = fetch_region(
            region["label"], region["cc"], known_urls, seen_slugs
        )
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
