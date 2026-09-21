import asyncio
import os
from datetime import timezone
from email.utils import parsedate_to_datetime
from itertools import product

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv
from googlenewsdecoder import gnews_decoder_async

from db import get_latest_timestamp, update_latest_timestamp, insert_articles

load_dotenv()

SOURCE_NAME = "GOOGLE_NEWS"
SCRAPER_ID = 84
COMPANY_ID = os.getenv("ERP_RECRUIT_COMPANY_ID")

KEYWORDS = [
    "Oracle JD Edwards",
    "Oracle Cloud",
    "Oracle EPM Cloud",
    "Oracle Fusion",
    "Oracle HCM Cloud",
    "Oracle E-Business Suite",
    "Oracle EBS",
    "Oracle JDE",
    "Oracle PeopleSoft",
    "Oracle ERP",
]

REGIONS = [
    {"label": "UK", "cc": "GB"},
    {"label": "NL", "cc": "NL"},
    {"label": "BE", "cc": "BE"},
    {"label": "IE", "cc": "IE"},
    {"label": "LU", "cc": "LU"},
]

WHEN = "when:1d"  # recency filter for the RSS query
RSS_URL = "https://news.google.com/rss/search?q={query}&hl=hl-{cc}&gl={cc}&ceid={cc}:en"

RSS_CONCURRENCY = 10
SCRAPE_CONCURRENCY = 5
DECODE_CONCURRENCY = 8
DECODE_TIMEOUT = 15
DECODE_RETRIES = 2
MIN_TEXT_LENGTH = 100

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-language": "en-US,en;q=0.9,fr;q=0.8,af;q=0.7,ar;q=0.6,be;q=0.5,de;q=0.4",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "sec-ch-ua": '"Not=A?Brand";v="99", "Google Chrome";v="131", "Chromium";v="131"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}

UNWANTED_SELECTORS = (
    "script, style, noscript, template, iframe, svg, form, nav, header, footer, "
    "aside, figure, picture, img, video, audio, button, input, select, textarea, "
    "label, [role='navigation'], [role='banner'], [role='complementary'], "
    "[aria-hidden='true'], .ad, .ads, .advert, .advertisement, .social, .share, "
    ".related, .recommended, .newsletter, .subscribe, .signup, .comments, "
    ".breadcrumb, .byline, .author, .tags, .pagination, .cookie, .paywall, "
    "[class*='most-read'], [id*='most-read'], [class*='mostread'], "
    "[class*='trending'], [id*='trending'], [class*='popular'], [id*='popular'], "
    "[class*='also-read'], [class*='read-next'], [class*='read-more'], "
    "[class*='related'], [class*='recommended'], [class*='sidebar'], "
    "[class*='editor-pick'], [class*='top-stories']"
)

BOILERPLATE_HEADINGS = {
    "most read", "trending", "trending now", "popular", "most popular",
    "top stories", "related articles", "related stories", "you may like",
    "you may also like", "you might also like", "also read", "read more",
    "don't miss", "in case you missed it", "latest news", "more news",
    "editor's picks", "editors' picks", "featured",
}

CANDIDATE_SELECTORS = (
    "[itemprop='articleBody']",
    "article",
    "[role='main']",
    "main",
    ".article-body, .article__body, .article-content, .articleBody, .body-copy, "
    ".story-body, .post-content, .entry-content, .content__article-body, "
    ".c-article-body, #article-body, #story, .story, .paywall",
)


# ----------------------------------------------------------
# HTTP fetch (with proxy, shared async session)
# ----------------------------------------------------------
async def fetch_url(session, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            resp = await session.get(
                url,
                headers=HEADERS,
                proxies=PROXIES,
                impersonate="chrome131",
                timeout=30,
            )
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} [{url[:60]}]: {e}")
                await asyncio.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {e}")
                return None


# ----------------------------------------------------------
# Parse RSS pubDate → naive UTC ISO ("2026-09-20T01:06:00")
# ----------------------------------------------------------
def parse_pub_date(pub_date):
    try:
        dt = parsedate_to_datetime(pub_date)
        return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
    except Exception:
        return None


def parse_rss_items(xml):
    soup = BeautifulSoup(xml, "xml")
    items = []
    for item in soup.find_all("item"):
        title_tag = item.find("title")
        link_tag = item.find("link")
        date_tag = item.find("pubDate")
        if not (title_tag and link_tag and date_tag):
            continue

        date = parse_pub_date(date_tag.get_text(strip=True))
        if not date:
            continue

        items.append(
            {
                "title": title_tag.get_text(strip=True),
                "gnews_url": link_tag.get_text(strip=True),
                "date": date,
            }
        )

    return items


# ----------------------------------------------------------
# Phase 1: Fetch Google News RSS feeds for all keyword x region combos
# ----------------------------------------------------------
async def fetch_rss_feeds():
    total = len(KEYWORDS) * len(REGIONS)
    sem = asyncio.Semaphore(RSS_CONCURRENCY)

    async def fetch_one(session, idx, region, keyword):
        query = f'"{keyword}" {WHEN}'
        url = RSS_URL.format(query=query.replace(" ", "+"), cc=region["cc"])
        async with sem:
            xml = await fetch_url(session, url)
        if not xml:
            print(f"  ⚠️  [{idx}/{total}] RSS failed: {region['label']} | {keyword}")
            return []
        items = parse_rss_items(xml)
        print(f"  📡 [{idx}/{total}] {region['label']} | {keyword:<28} | {len(items)}")
        return items

    async with AsyncSession() as session:
        tasks = [
            fetch_one(session, idx, region, keyword)
            for idx, (region, keyword) in enumerate(product(REGIONS, KEYWORDS), 1)
        ]
        results = await asyncio.gather(*tasks)

    return [item for items in results for item in items]


# ----------------------------------------------------------
# Scrape a single article page — body text only
# ----------------------------------------------------------
def remove_boilerplate_sections(soup):
    # Remove "Most Read" / "Trending" style blocks: heading + following siblings
    for heading in soup.find_all(["h2", "h3", "h4"]):
        if heading.get_text(strip=True).lower() in BOILERPLATE_HEADINGS:
            sibling = heading.find_next_sibling()
            while sibling:
                next_sibling = sibling.find_next_sibling()
                sibling.decompose()
                sibling = next_sibling
            heading.decompose()


def extract_text(soup):
    for tag in soup.select(UNWANTED_SELECTORS):
        tag.decompose()
    remove_boilerplate_sections(soup)

    candidates = []
    for selector in CANDIDATE_SELECTORS:
        candidates.extend(soup.select(selector))

    if candidates:
        container = max(candidates, key=lambda n: len(n.get_text(strip=True)))
    else:
        # Fallback: pick the div/section with the most paragraph text
        container, best_score = None, 0
        for node in soup.find_all(["div", "section"]):
            score = sum(len(p.get_text(strip=True)) for p in node.find_all("p"))
            if score > best_score:
                container, best_score = node, score

    if not container:
        return ""

    return " ".join(container.get_text(" ", strip=True).split())


async def scrape_article(article, sem):
    # Fresh session per article — no shared cookies/state across sites
    async with sem:
        async with AsyncSession() as session:
            html = await fetch_url(session, article["url"])
            if not html:
                return None

            soup = BeautifulSoup(html, "html.parser")
            return {**article, "text": extract_text(soup)}


async def scrape_articles(articles):
    print(f"\n⚡ Scraping {len(articles)} article(s) with concurrency {SCRAPE_CONCURRENCY}...")
    sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)

    results = await asyncio.gather(
        *(scrape_article(article, sem) for article in articles),
        return_exceptions=True,
    )

    scraped = []
    for article, result in zip(articles, results):
        if isinstance(result, Exception):
            print(f"  ⚠️  Exception scraping {article['url']}: {result}")
            continue

        if not result or len(result.get("text", "")) < MIN_TEXT_LENGTH:
            title = result["title"] if result else article["title"]
            print(f"  ⚠️  No usable text: {title[:60]}")
            continue

        scraped.append(result)
        print(f"  ✅ {result['title'][:70]}...")

    return scraped


# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
async def async_main():
    print("🔍 Google News scraper starting...")
    print(f"Keywords: {len(KEYWORDS)} | Regions: {len(REGIONS)}")

    saved_timestamp = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)
    print(f"🗄️  Saved timestamp: {saved_timestamp or 'None (first run)'}")

    # Phase 1: collect RSS items (newest first)
    print("\nFetching Google News RSS feeds...")
    items = await fetch_rss_feeds()
    print(f"\nRaw results: {len(items)}")

    if not items:
        print("⛔ No articles found.")
        return

    # Sort by latest
    items.sort(key=lambda a: a["date"], reverse=True)
    newest_timestamp = items[0]["date"]

    # First run — save timestamp only
    if saved_timestamp is None:
        print("🟢 First run detected — NOT scraping any articles.")
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        print("🕒 Saved latest timestamp:", newest_timestamp)
        return

    print("Previously saved timestamp:", saved_timestamp)

    # Keep only articles newer than saved timestamp
    items = [a for a in items if a["date"] > saved_timestamp]
    if not items:
        print("⛔ No new articles found.")
        return

    # Deduplicate: exact Google News URL first, then title
    # (same story appears across keywords/regions, sometimes with a different ID)
    seen_gnews_urls = set()
    seen_titles = set()
    unique_items = []
    for item in items:
        if item["gnews_url"] in seen_gnews_urls:
            continue
        title_key = item["title"].strip().lower()
        if title_key in seen_titles:
            continue
        seen_gnews_urls.add(item["gnews_url"])
        seen_titles.add(title_key)
        unique_items.append(item)

    print(f"🆕 {len(unique_items)} new article(s) after title deduplication.")

    # Phase 2: decode Google News URLs (with retry on failures)
    print(f"\nDecoding {len(unique_items)} Google News URLs...")
    gnews_urls = [a["gnews_url"] for a in unique_items]
    decoded_results = await gnews_decoder_async(
        gnews_urls,
        timeout=DECODE_TIMEOUT,
        concurrency=DECODE_CONCURRENCY,
    )

    for retry in range(DECODE_RETRIES):
        failed = [i for i, r in enumerate(decoded_results) if not r.get("success")]
        if not failed:
            break
        print(f"  🔁 Retrying {len(failed)} failed decode(s) ({retry + 1}/{DECODE_RETRIES})...")
        await asyncio.sleep(2)
        retry_results = await gnews_decoder_async(
            [gnews_urls[i] for i in failed],
            timeout=DECODE_TIMEOUT,
            concurrency=DECODE_CONCURRENCY,
        )
        for i, result in zip(failed, retry_results):
            decoded_results[i] = result

    articles = []
    seen_urls = set()
    for item, result in zip(unique_items, decoded_results):
        if not result.get("success"):
            print(f"  ⚠️  Decode failed: {item['title'][:60]} — {result.get('message', '')}")
            continue
        url = result["decoded_url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)
        articles.append(
            {
                "url": url,
                "title": item["title"],
                "date": item["date"],
                "lastmod": item["date"],
                "scraper_id": SCRAPER_ID,
            }
        )

    print(f"🔗 {len(articles)} unique article URL(s) after decoding.")

    if not articles:
        print("⛔ No articles to scrape.")
        return

    # Phase 3: scrape article pages for text
    articles = await scrape_articles(articles)
    if not articles:
        print("\n⛔ No articles scraped successfully.")
        return

    print(f"\n🆕 Found {len(articles)} new article(s) with text.")

    inserted = insert_articles([dict(a, company_id=COMPANY_ID) for a in articles])
    print(f"✅ Inserted {inserted} article(s) into database")

    update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
