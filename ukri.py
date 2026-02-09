import requests
from bs4 import BeautifulSoup
import time
from db import get_latest_timestamp, update_latest_timestamp, insert_articles

MAIN_SITEMAP = "https://www.ukri.org/sitemap.xml"
SOURCE_NAME = "UKRI"

headers = {"User-Agent": "Mozilla/5.0"}


# ----------------------------------------------------------
# Scrape a single article
# ----------------------------------------------------------
def scrape_article(url):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            time.sleep(2)  # Sleep 2 seconds before request
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for {url}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed to fetch {url} after {max_retries} attempts")
                return None
    
    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract title from h1
    title_tag = soup.select_one("h1.main-area__page-title")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Extract date from time tag
    date = ""
    date_tag = soup.select_one("time")
    if date_tag:
        date = date_tag.get("datetime", "") or date_tag.get_text(strip=True)
    
    # Get the entry-content div which contains the article text
    content_div = soup.select_one(".entry-content")
    if not content_div:
        return None

    # Remove unwanted elements
    cleanup_selectors = [
        "script",
        "style",
        "img",
        "svg",
        ".share-this-page__container",
        ".clear-content",
        ".widget",
        "form",
        "iframe",
        "video",
    ]
    for sel in cleanup_selectors:
        for tag in content_div.select(sel):
            tag.decompose()

    # Extract all text from content
    text = content_div.get_text(" ", strip=True)
    # Clean up extra whitespace
    text = " ".join(text.split())

    return {
        "source": SOURCE_NAME,
        "group_name": "2",
        "url": url,
        "date": date,
        "title": title,
        "text": text,
        "company_id": "234f37eb-1147-43fb-89c1-9812e0824e1f",
    }


# ----------------------------------------------------------
# Get the latest "news" sitemap
# ----------------------------------------------------------
def get_latest_news_sitemap():
    max_retries = 3
    for attempt in range(max_retries):
        try:
            time.sleep(2)  # Sleep 2 seconds before request
            resp = requests.get(MAIN_SITEMAP, headers=headers, timeout=30)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for sitemap: {str(e)}")
                time.sleep(2)
            else:
                raise Exception(f"Failed to fetch sitemap after {max_retries} attempts: {str(e)}")
    
    soup = BeautifulSoup(resp.text, "xml")

    links = []
    for loc in soup.find_all("loc"):
        link = loc.text.strip()
        if "news-sitemap" in link:
            links.append(link)

    if not links:
        raise Exception("No news sitemap links found.")

    # Extract number from URLs like "news-sitemap2.xml" or "news-sitemap.xml" (no number = 1)
    def get_sitemap_number(url):
        if "news-sitemap.xml" in url and "news-sitemap2" not in url:
            return 1  # First sitemap has no number
        try:
            # Extract number between "news-sitemap" and ".xml"
            num = url.split("news-sitemap")[1].split(".xml")[0]
            return int(num) if num else 1
        except:
            return 0

    links.sort(key=get_sitemap_number)
    return links[-1]  # Return the one with highest number


# ----------------------------------------------------------
# Read article URLs + lastmod timestamps
# ----------------------------------------------------------
def get_articles_from_sitemap(sitemap_url):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            time.sleep(2)  # Sleep 2 seconds before request
            resp = requests.get(sitemap_url, headers=headers, timeout=30)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries} for sitemap URL: {str(e)}")
                time.sleep(2)
            else:
                raise Exception(f"Failed to fetch sitemap URL after {max_retries} attempts: {str(e)}")
    
    soup = BeautifulSoup(resp.text, "xml")

    articles = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        lastmod = url_tag.find("lastmod")
        
        if loc:
            url = loc.text.strip()
            timestamp = lastmod.text.strip() if lastmod else ""
            articles.append({"url": url, "lastmod": timestamp})

    return articles


# ----------------------------------------------------------
# MAIN LOGIC
# ----------------------------------------------------------
def main():
    saved_timestamp = get_latest_timestamp(SOURCE_NAME)

    print("🔍 Fetching main sitemap...")
    latest_sitemap = get_latest_news_sitemap()
    print("Using sitemap:", latest_sitemap)

    article_entries = get_articles_from_sitemap(latest_sitemap)
    article_entries.sort(key=lambda x: x["lastmod"], reverse=True)

    newest_timestamp = article_entries[0]["lastmod"] if article_entries else ""

    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT scraping any articles.")
        print("Saving latest timestamp:", newest_timestamp)
        update_latest_timestamp(SOURCE_NAME, newest_timestamp)
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
    update_latest_timestamp(SOURCE_NAME, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
