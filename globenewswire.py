import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles, is_subscription_active

load_dotenv()

BASE_URL = "https://www.globenewswire.com"
LISTING_URL = (
    "https://www.globenewswire.com/en/search/continent/eu"
    "/subject/coa,prs,fin,mgt,mgc,mna,pdt,prt"
    "/industry/technology,software,software%2520&%2520computer%2520services,"
    "internet,computer%2520services,financial%2520services,"
    "financial%2520data%2520providers,business%252520support%252520services"
    "/load/more?page={page}&pageSize=50"
)
SOURCE_NAME = "GLOBENEWSWIRE"
SCRAPER_ID = 52
MAX_PAGES = 3
MAX_THREADS = 5

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

COMPANY_CONFIGS = [
    {
        "label": "H2 Recruit",
        "company_id": os.getenv("H2_RECRUIT_COMPANY_ID"),
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}


# ----------------------------------------------------------
# HTTP fetch (with proxy)
# ----------------------------------------------------------
def fetch_url(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} [{url[:60]}]: {e}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


# ----------------------------------------------------------
# Parse listing page → [(url, date_str), ...]  (English only)
# ----------------------------------------------------------
def parse_listing_page(html):
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen = set()

    for li in soup.select("li.row"):
        link_tag = li.select_one(".mainLink a[href]")
        if not link_tag:
            continue

        href = link_tag.get("href", "")
        # Path: /news-release/{year}/{month}/{day}/{id}/0/{lang}/slug
        parts = href.rstrip("/").split("/")
        lang = parts[7] if len(parts) > 7 else ""
        if lang != "en":
            continue

        full_url = BASE_URL + href if href.startswith("/") else href
        if full_url in seen:
            continue
        seen.add(full_url)

        date_span = li.select_one(".date-source span")
        date_str = date_span.get_text(strip=True) if date_span else ""

        results.append((full_url, date_str))

    return results


# ----------------------------------------------------------
# Parse listing date → approximate UTC ISO for early-break check
# ----------------------------------------------------------
def parse_listing_date(date_str):
    try:
        for tz in (" ET", " EDT", " EST", " PT", " PDT", " PST", " GMT", " BST", " UTC"):
            date_str = date_str.replace(tz, "")
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y %H:%M")
        return (dt + timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


# ----------------------------------------------------------
# Phase 1: Collect article URLs from listing pages
# ----------------------------------------------------------
def collect_links(saved_timestamp):
    collected = []

    for page_num in range(1, MAX_PAGES + 1):
        url = LISTING_URL.format(page=page_num)
        print(f"\n📄 Fetching listing page {page_num}...")

        html = fetch_url(url)
        if not html:
            print(f"⛔ Could not fetch page {page_num}, stopping.")
            break

        items = parse_listing_page(html)
        print(f"📋 Found {len(items)} English article(s) on page {page_num}.")

        if not items:
            break

        stop = False
        for article_url, date_str in items:
            if saved_timestamp:
                approx_ts = parse_listing_date(date_str)
                if approx_ts and approx_ts <= saved_timestamp:
                    print(f"🛑 Reached old article ('{date_str}') — stopping pagination.")
                    stop = True
                    break
            collected.append((article_url, date_str))
            print(f"    🔗 {article_url}")

        if stop:
            break

    return collected


# ----------------------------------------------------------
# Scrape a single article page
# ----------------------------------------------------------
def scrape_article(url):
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.select_one("h1.article-headline")
    title = h1.get_text(" ", strip=True) if h1 else ""
    if not title:
        og = soup.find("meta", property="og:title")
        title = og["content"].strip() if og and og.get("content") else ""

    # Timestamp from <time datetime="2026-06-04T06:30:00Z">
    timestamp = ""
    time_tag = soup.select_one("time[datetime]")
    if time_tag:
        dt_attr = time_tag.get("datetime", "")
        timestamp = dt_attr.rstrip("Z")
        if len(timestamp) == 16:
            timestamp += ":00"

    # Fallback: itemprop meta tags
    if not timestamp:
        for prop in ("datePublished", "dateModified"):
            meta = soup.find("meta", itemprop=prop)
            if meta and meta.get("content"):
                timestamp = meta["content"].rstrip("Z")[:19]
                break

    # Body text
    body_div = (
        soup.select_one("div.main-body-container.article-body")
        or soup.select_one("#main-body-container")
    )
    if body_div:
        for tag in body_div.select("script, style, iframe, .social-media-side-bar-container"):
            tag.decompose()
        text = " ".join(body_div.get_text(" ", strip=True).split())
    else:
        text = ""

    return {
        "url": url,
        "title": title,
        "text": text,
        "date": timestamp,
        "lastmod": timestamp,
        "scraper_id": SCRAPER_ID,
    }


# ----------------------------------------------------------
# Phase 2: Scrape articles concurrently (max 5 threads)
# ----------------------------------------------------------
def scrape_articles_threaded(links, saved_timestamp):
    articles = []
    newest_timestamp = None

    urls = [url for url, _ in links]
    print(f"\n🧵 Scraping {len(urls)} article(s) with {MAX_THREADS} threads...")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_url = {executor.submit(scrape_article, url): url for url in urls}

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                article = future.result()
            except Exception as e:
                print(f"  ⚠️  Exception scraping {url}: {e}")
                continue

            if not article:
                print(f"  ⚠️  Failed to scrape: {url}")
                continue

            if not article.get("date"):
                print(f"  ⚠️  No timestamp for: {url}")
                continue

            if newest_timestamp is None or article["date"] > newest_timestamp:
                newest_timestamp = article["date"]

            if saved_timestamp and article["date"] <= saved_timestamp:
                print(f"  ⏭️  Old article, skipping: {article['title'][:60]}")
                continue

            articles.append(article)
            print(f"  ✅ {article['title'][:70]}...")

    return articles, newest_timestamp


# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
def main():
    company_id = COMPANY_CONFIGS[0]["company_id"]
    saved_timestamp = get_latest_timestamp(SCRAPER_ID, company_id)

    print("🔍 GlobeNewswire scraper starting...")
    print(f"🗄️  Saved timestamp: {saved_timestamp or 'None (first run)'}")

    # Phase 1: collect links
    links = collect_links(saved_timestamp)
    print(f"\n🔗 Total articles to scrape: {len(links)}")

    if not links:
        print("⛔ No articles found.")
        return

    # Phase 2: threaded article scraping
    all_articles, newest_timestamp = scrape_articles_threaded(links, saved_timestamp)

    # Save results per company
    for config in COMPANY_CONFIGS:
        company_id = config["company_id"]
        label = config["label"]

        print(f"\n{'='*60}")
        print(f"🏢 Processing: {label}")
        print(f"{'='*60}")

        if not is_subscription_active(SCRAPER_ID, company_id):
            print(f"⏭️  Skipping {label} — subscription is inactive")
            continue

        saved_ts = get_latest_timestamp(SCRAPER_ID, company_id)

        if saved_ts is None:
            print("🟢 First run detected — NOT saving any articles.")
            if newest_timestamp:
                update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
                print("🕒 Saved latest timestamp:", newest_timestamp)
            continue

        print("Previously saved timestamp:", saved_ts)

        new_articles = [a for a in all_articles if a["date"] > saved_ts]

        if not new_articles:
            print("⛔ No new articles found.")
            continue

        print(f"🆕 Found {len(new_articles)} new article(s).")

        company_articles = [dict(a, company_id=company_id) for a in new_articles]
        inserted = insert_articles(company_articles)
        print(f"✅ Inserted {inserted} article(s) for {label}")

        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
