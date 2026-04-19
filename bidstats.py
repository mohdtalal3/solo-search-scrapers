import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles

load_dotenv()

SOURCE_NAME = "BIDSTATS"
SCRAPER_ID = 23
COMPANY_ID = os.getenv("PLEA_COMPANY_ID")

BASE_URL = "https://bidstats.uk"
SEARCH_ENDPOINT = "https://bidstats.uk/tenders/"
SEARCH_PARAMS = {
    "q": (
        '"landscape architect" OR "landscape architecture" OR "landscape design" OR '
        '"external works design" OR "public realm" OR "planting scheme" OR '
        '"soft landscaping" OR "hard landscaping" OR "biodiversity net gain" OR '
        'BNG OR "green infrastructure" OR "ecological enhancement" OR '
        '"grounds maintenance" OR "grounds management"'
    ),
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
}

# Sections to strip before converting article to plain text
REMOVE_SELECTORS = [
    "#notice-concepts",  # Related Terms (noisy keyword cloud)
    "#notice-location",  # Location section containing the Leaflet map
    "#geomap",           # Leaflet map container (belt-and-braces)
    "img",               # All images
    "script",            # Inline scripts
    "nav",               # Back/prev/next navigation
]


def url_slug(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1]


def current_month_label() -> str:
    """Return e.g. 'April 2026' matching the dategroup headings on bidstats."""
    return datetime.now().strftime("%B %Y")


SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"


def scrappey_get(url: str) -> str:
    """
    Fetch a URL via Scrappey (JS-rendered).
    Falls back to plain requests if SCRAPPEY_API_KEY is not set.
    """
    scrappey_key = os.getenv("SCRAPPEY_API_KEY")
    if scrappey_key:
        payload = {
            "cmd": "request.get",
            #"requestType": "request",
            "retries": 2,
            "url": url,
            "premiumProxy": True,
            "proxyCountry": "UnitedKingdom",
        }
        resp = requests.post(
            f"{SCRAPPEY_API_URL}?key={scrappey_key}",
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json().get("solution", {}).get("response", "")
    else:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.text


def fetch_listing() -> list[tuple[str, str]]:
    """
    Fetch the search results page via Scrappey and return (full_url, title)
    for every notice published in the current calendar month.
    """
    target_month = current_month_label()
    # Build the full search URL manually (Scrappey takes a single URL string)
    from urllib.parse import urlencode
    search_url = f"{SEARCH_ENDPOINT}?{urlencode(SEARCH_PARAMS)}"
    try:
        time.sleep(1)
        html = scrappey_get(search_url)
        soup = BeautifulSoup(html, "html.parser")

        batch = soup.select_one("div.nl-batch")
        if not batch:
            print("  ❌ Could not find div.nl-batch on listing page.")
            return []

        items: list[tuple[str, str]] = []
        in_current_month = False

        for child in batch.children:
            # Skip plain text / whitespace nodes
            if not hasattr(child, "name") or child.name is None:
                continue

            if child.name == "h2" and "dategroup" in child.get("class", []):
                month_text = child.get_text(strip=True)
                in_current_month = (month_text == target_month)
                # Once we've collected the current month and hit the next, stop.
                if not in_current_month and items:
                    break

            elif (
                child.name == "div"
                and "noticegrid" in child.get("class", [])
                and in_current_month
            ):
                for a in child.select("li.noticebox div.nbx-title a"):
                    href = a.get("href", "")
                    if not href:
                        continue
                    title = a.get_text(strip=True)
                    full_url = BASE_URL + href if href.startswith("/") else href
                    items.append((full_url, title))

        print(f"  📄 Found {len(items)} notices for {target_month}.")
        return items

    except Exception as e:
        print(f"❌ Failed to fetch listing: {e}")
        return []


def scrape_notice(url: str, fallback_title: str, max_retries: int = 3):
    """
    Scrape title, published date, and body text from a bidstats notice page.
    Body is the full article#view-notice text with map/images removed.
    """
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            article = soup.select_one("article#view-notice")
            if not article:
                return fallback_title, "", ""

            # Title
            h1 = article.select_one("h1")
            title = h1.get_text(" ", strip=True) if h1 else fallback_title

            # Published date from keydata (e.g. "13 Apr 2026")
            date = ""
            for dt_el in article.select("#notice-keydata dt"):
                if "Published" in dt_el.get_text():
                    dd_el = dt_el.find_next_sibling("dd")
                    if dd_el:
                        raw = dd_el.get_text(" ", strip=True)
                        try:
                            date = datetime.strptime(raw, "%d %b %Y").date().isoformat()
                        except ValueError:
                            date = raw
                    break

            # Strip unwanted elements before extracting text
            for sel in REMOVE_SELECTORS:
                for el in article.select(sel):
                    el.decompose()

            body = article.get_text("\n", strip=True)
            return title, date, body

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(2)
            else:
                print(f"  ❌ Failed to scrape {url}: {e}")
                return None, None, None


def main():
    print("🔍 Fetching Bidstats landscape / grounds tenders...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")

    seen_slugs = {url_slug(u) for u in known_urls}

    items = fetch_listing()
    
    new_items = []
    for full_url, title in items:
        slug = url_slug(full_url)
        if full_url in known_urls:
            print(f"  ⏭️  Skipping (already in DB): {slug}")
            continue
        if slug in seen_slugs:
            print(f"  ⏭️  Skipping (duplicate slug): {slug}")
            continue
        new_items.append((full_url, title))
        seen_slugs.add(slug)

    if not new_items:
        print("\n⛔ No new notices found.")
        return

    print(f"  🆕 {len(new_items)} new notice(s) to scrape.")
    #new_items=new_items[:5]  # TEMP LIMIT FOR TESTING - REMOVE THIS LATER
    articles = []
    for full_url, fallback_title in new_items:
        print(f"  Scraping: {full_url}")
        title, date, body = scrape_notice(full_url, fallback_title)
        if title is None:
            continue
        articles.append({
            "url": full_url,
            "date": date,
            "title": title,
            "text": body,
            "company_id": COMPANY_ID,
            "scraper_id": SCRAPER_ID,
        })
        print(f"  ✅ {title[:60]}...")

    if not articles:
        print("\n⛔ No notices scraped successfully.")
        return

    print(f"\n🆕 Found {len(articles)} new notice(s) in total.")
    inserted_count = insert_articles(articles)
    print(f"✅ Inserted {inserted_count} notices into database")


if __name__ == "__main__":
    main()
