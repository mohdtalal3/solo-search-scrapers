import os
import time

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import get_latest_timestamp, update_latest_timestamp, insert_articles

MAIN_SITEMAP = "https://www.themanufacturer.com/sitemap_index.xml"
SOURCE_NAME = "THE_MANUFACTURER"
SCRAPER_ID = 9
COMPANY_ID = os.getenv("ARDEN_EXEC_COMPANY_ID")
SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
SCRAPPEY_PROXY_COUNTRY = "UnitedKingdom"

load_dotenv()


# ----------------------------------------------------------
# Fetch a URL through Scrappey with retry logic
# ----------------------------------------------------------
def fetch_with_scrappey(url, max_retries=3):
    scrappey_api_key = os.getenv("SCRAPPEY_API_KEY")
    if not scrappey_api_key:
        raise RuntimeError("Please set SCRAPPEY_API_KEY in your environment")

    payload = {
        "cmd": "request.get",
        "requestType": "request",
        "url": url,
        "proxyCountry": SCRAPPEY_PROXY_COUNTRY,
        "premiumProxy": True,
    }

    for attempt in range(max_retries):
        try:
            time.sleep(2)
            response = requests.post(
                f"{SCRAPPEY_API_URL}?key={scrappey_api_key}",
                json=payload,
                timeout=90,
            )
            response.raise_for_status()

            scrappey_response = response.json()
            solution = scrappey_response.get("solution", {})
            status_code = solution.get("statusCode")

            if status_code and status_code != 200:
                print(f"❌ Scrappey returned status {status_code} for {url}")
                return None

            if scrappey_response.get("data") == "error" or not solution.get("verified", False):
                error_message = scrappey_response.get("error", "Unknown Scrappey error")
                raise RuntimeError(error_message)

            return solution.get("response") or ""

        except (requests.RequestException, RuntimeError) as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {str(e)}")
                return None


# ----------------------------------------------------------
# Scrape a single article
# ----------------------------------------------------------
def scrape_article(url):
    html = fetch_with_scrappey(url)
    if not html:
        print(f"❌ Failed to fetch {url}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    # -----------------------------
    # TITLE
    # -----------------------------
    title_tag = soup.select_one("h1.page-title span")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # -----------------------------
    # DATE
    # -----------------------------
    date_tag = soup.select_one("#single-article-date")
    date = date_tag.get_text(strip=True) if date_tag else ""

    # -----------------------------
    # CATEGORIES
    # -----------------------------
    category_nodes = soup.select(".article-categories a")
    categories = [c.get_text(strip=True) for c in category_nodes]

    # -----------------------------
    # TAGS
    # -----------------------------
    tag_nodes = soup.select(".article-tags a")
    tags = [t.get_text(strip=True) for t in tag_nodes]

    # -----------------------------
    # MAIN ARTICLE CONTENT
    # -----------------------------
    content_div = soup.select_one(".post-content")
    if not content_div:
        return None

    paragraphs = [p.get_text(" ", strip=True) for p in content_div.find_all("p") if p.get_text(strip=True)]
    text = "\n\n".join(paragraphs)

    return {
        "url": url,
        "date": date,
        "title": title,
        "categories": categories,
        "tags": tags,
        "text": text,
        "company_id": COMPANY_ID,
        "scraper_id": SCRAPER_ID
    }


# ----------------------------------------------------------
# Get the latest "articles-sitemap" from the main sitemap
# ----------------------------------------------------------
def get_latest_articles_sitemap():
    html = fetch_with_scrappey(MAIN_SITEMAP)
    print(html)
    if not html:
        raise Exception("Failed to fetch main sitemap via Scrappey.")
    soup = BeautifulSoup(html, "xml")
    links = []
    for sitemap in soup.find_all("sitemap"):
        loc = sitemap.find("loc")
        if loc:
            link = loc.text.strip()
            if "articles-sitemap" in link:
                links.append(link)

    if not links:
        raise Exception("No articles-sitemap links found.")

    # Extract trailing number from URLs like "articles-sitemap26.xml"
    def get_sitemap_number(url):
        try:
            num = url.split("articles-sitemap")[1].split(".xml")[0]
            return int(num) if num else 0
        except Exception:
            return 0

    links.sort(key=get_sitemap_number)
    return links[-1]  # Return the one with the highest number


# ----------------------------------------------------------
# Read article URLs + lastmod timestamps from a sitemap
# ----------------------------------------------------------
def get_articles_from_sitemap(sitemap_url):
    html = fetch_with_scrappey(sitemap_url)
    if not html:
        raise Exception(f"Failed to fetch sitemap via Scrappey: {sitemap_url}")
    soup = BeautifulSoup(html, "xml")

    articles = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        lastmod = url_tag.find("lastmod")
        if loc and lastmod:
            articles.append({"url": loc.text, "lastmod": lastmod.text})

    return articles


# ----------------------------------------------------------
# MAIN LOGIC
# ----------------------------------------------------------
def main():
    saved_timestamp = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)

    print("🔍 Fetching main sitemap...")
    latest_sitemap = get_latest_articles_sitemap()
    print("Using sitemap:", latest_sitemap)

    article_entries = get_articles_from_sitemap(latest_sitemap)
    article_entries.sort(key=lambda x: x["lastmod"], reverse=True)

    newest_timestamp = article_entries[0]["lastmod"]

    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT scraping any articles.")
        print("Saving latest timestamp:", newest_timestamp)
        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        return

    # ----------------------------
    # SUBSEQUENT RUNS — scrape new
    # ----------------------------
    print("Previously saved timestamp:", saved_timestamp)

    new_articles = [a for a in article_entries if a["lastmod"] > saved_timestamp]

    if not new_articles:
        print("⛔ No new articles found.")
        return

    print(f"🆕 Found {len(new_articles)} new articles.")

    scraped_articles = []
    for article in new_articles:
        print("Scraping:", article["url"])
        scraped = scrape_article(article["url"])
        if scraped:
            scraped["lastmod"] = article["lastmod"]
            scraped_articles.append(scraped)

    # Insert articles into database
    if scraped_articles:
        inserted_count = insert_articles(scraped_articles)
        print(f"✅ Inserted {inserted_count} articles into database")

    # Update timestamp
    update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()