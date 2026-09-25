import asyncio
import os
from datetime import datetime

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

from db import get_recent_article_urls, insert_articles

load_dotenv()

LISTING_URL = "https://www.builtinboston.com/articles"
SOURCE_NAME = "BUILTIN_BOSTON"
SCRAPER_ID = 87
COMPANY_ID = os.getenv("TALENT_TO_HIRE")
SCRAPE_CONCURRENCY = 5

PROXY = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


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


def url_slug(url):
    return url.rstrip("/").rsplit("/", 1)[-1]


def parse_listing_date(date_text):
    """'Published on September 25, 2026' / 'Updated on ...' → ISO"""
    try:
        date_part = date_text.split(" on ", 1)[1]
        return datetime.strptime(date_part.strip(), "%B %d, %Y").strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return ""


def parse_listing_items(html):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for container in soup.select("div.border-top.border-gray-02"):
        link = container.select_one("a.hover-underline[href*='/articles/']")
        if not link:
            continue

        article_url = link.get("href", "")
        if not article_url.startswith("http"):
            article_url = "https://www.builtinboston.com" + article_url

        date_node = container.find(string=lambda t: t and " on " in t and "," in t)
        date = parse_listing_date(date_node.strip()) if date_node else ""

        items.append({"url": article_url, "date": date})

    return items


def extract_text(html):
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else ""

    content = soup.select_one("div.article-content")
    if not content:
        return title, ""

    for tag in content.select(
        "script, style, noscript, iframe, picture, img, figure, "
        ".bix-embed-read-more, .open-jobs, .snippet-box"
    ):
        tag.decompose()

    text = " ".join(content.get_text(" ", strip=True).split())
    return title, text


async def scrape_article(item, sem):
    # Fresh session per article — no shared cookies/state across pages
    async with sem:
        async with AsyncSession() as session:
            html = await fetch_url(session, item["url"])
            if not html:
                return None

            title, text = extract_text(html)
            if not text:
                return None

            return {
                "url": item["url"],
                "title": title,
                "text": text,
                "date": item["date"],
                "lastmod": item["date"],
                "scraper_id": SCRAPER_ID,
            }


async def scrape_articles(items):
    print(f"\n⚡ Scraping {len(items)} article(s) with concurrency {SCRAPE_CONCURRENCY}...")
    sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)
    results = await asyncio.gather(
        *(scrape_article(item, sem) for item in items),
        return_exceptions=True,
    )

    articles = []
    for item, result in zip(items, results):
        if isinstance(result, Exception):
            print(f"  ⚠️  Exception scraping {item['url']}: {result}")
            continue
        if not result:
            print(f"  ⚠️  Failed to scrape: {item['url']}")
            continue
        articles.append(result)
        print(f"  ✅ {result['title'][:70]}")

    return articles


async def async_main():
    print("🔍 Fetching Built In Boston articles...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")

    print("📄 Fetching listing page...")
    async with AsyncSession() as session:
        listing_html = await fetch_url(session, LISTING_URL)
    if not listing_html:
        print("⛔ Could not fetch listing page.")
        return

    items = parse_listing_items(listing_html)
    print(f"   {len(items)} article(s) found.")

    seen_slugs = {url_slug(u) for u in known_urls}
    new_items = []
    for item in items:
        slug = url_slug(item["url"])
        if item["url"] in known_urls or slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        new_items.append(item)

    if not new_items:
        print("⛔ No new articles found.")
        return

    print(f"\n🆕 {len(new_items)} new article(s) to scrape.")

    articles = await scrape_articles(new_items)
    if not articles:
        print("⛔ No articles scraped successfully.")
        return

    print(f"\n🆕 Found {len(articles)} new article(s).")
    inserted = insert_articles([dict(a, company_id=COMPANY_ID) for a in articles])
    print(f"✅ Inserted {inserted} articles into database")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
