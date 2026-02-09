import requests
from bs4 import BeautifulSoup
import json
import os
import time
from db import get_latest_timestamp, update_latest_timestamp, insert_articles

MAIN_SITEMAP = "https://startups.co.uk/sitemap_index.xml"
SOURCE_NAME = "STARTUPS_CO"

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

    # Get the article tag
    article_tag = soup.select_one("article")
    if not article_tag:
        return None

    # Extract title
    title_tag = article_tag.select_one(".entry-header h1.entry-header-title")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Extract date
    date_tag = article_tag.select_one(".article-date-info .meta-value")
    date = date_tag.get_text(strip=True) if date_tag else ""

    # Remove unwanted elements from article
    cleanup_selectors = [
        "script",
        "style",
        "img",
        "svg",
        ".share-buttons",
        ".widget",
        "form",
        ".news_letter",
        ".mc4wp-form",
        ".featured-img",
        ".article-author-image",
    ]
    for sel in cleanup_selectors:
        for tag in article_tag.select(sel):
            tag.decompose()

    # Extract all text from article
    text = article_tag.get_text(" ", strip=True)
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
# Get the latest "post" sitemap
# ----------------------------------------------------------
def get_latest_post_sitemap():
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
    for sitemap in soup.find_all("sitemap"):
        loc = sitemap.find("loc")
        if loc:
            link = loc.text.strip()
            if "post-sitemap" in link:
                links.append(link)

    if not links:
        raise Exception("No post sitemap links found.")

    # Extract number from URLs like "post-sitemap2.xml" or "post-sitemap.xml" (no number = 1)
    def get_sitemap_number(url):
        if "post-sitemap.xml" in url and "post-sitemap2" not in url:
            return 1  # First sitemap has no number
        try:
            # Extract number between "post-sitemap" and ".xml"
            num = url.split("post-sitemap")[1].split(".xml")[0]
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
        loc = url_tag.find("loc").text
        lastmod = url_tag.find("lastmod").text
        articles.append({"url": loc, "lastmod": lastmod})

    return articles


# ----------------------------------------------------------
# MAIN LOGIC
# ----------------------------------------------------------
def main():
    saved_timestamp = get_latest_timestamp(SOURCE_NAME)

    print("🔍 Fetching main sitemap...")
    latest_sitemap = get_latest_post_sitemap()
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
