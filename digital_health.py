import requests
from bs4 import BeautifulSoup
import json
import os
from db import get_latest_timestamp, update_latest_timestamp, insert_articles

MAIN_SITEMAP = "https://www.digitalhealth.net/sitemap_index.xml"
SOURCE_NAME = "DIGITAL_HEALTH"
SCRAPER_ID = 3
COMPANY_ID = os.getenv("SOLO_SEARCH_COMPANY_ID")

headers = {"User-Agent": "Mozilla/5.0"}


# ----------------------------------------------------------
# Scrape a single article
# ----------------------------------------------------------
def scrape_article(url):
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.text, "html.parser")

    # -----------------------------
    # TITLE
    # -----------------------------
    title_tag = soup.select_one(".single_post_heading1 h1")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # -----------------------------
    # DATE (inside .page_comments)
    # -----------------------------
    date_tag = soup.select_one(".page_comments li")
    date = date_tag.get_text(strip=True) if date_tag else ""

    # -----------------------------
    # CATEGORIES (Digital Transformation, News, Smart Health)
    # -----------------------------
    category_nodes = soup.select(".page_category h4 a")
    categories = [c.get_text(strip=True) for c in category_nodes]

    # -----------------------------
    # TAGS (3D printing, NBT, etc.)
    # -----------------------------
    tag_nodes = soup.select(".tags ul li a")
    tags = [t.get_text(strip=True) for t in tag_nodes]

    # -----------------------------
    # MAIN ARTICLE CONTENT
    # -----------------------------
    content_div = soup.select_one("div.content")
    if not content_div:
        return None

    # Remove irrelevant / noisy sections
    cleanup_selectors = [
        ".summarising-content",
        "script",
        ".elementor",
        ".news_letter",
        ".mc4wp-form",
        "form",
    ]
    for sel in cleanup_selectors:
        for tag in content_div.select(sel):
            tag.decompose()

    # Extract paragraphs
    paragraphs = [p.get_text(" ", strip=True) for p in content_div.find_all("p")]
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
# Get the latest "post" sitemap
# ----------------------------------------------------------
def get_latest_post_sitemap():
    resp = requests.get(MAIN_SITEMAP, headers=headers)
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
    resp = requests.get(sitemap_url, headers=headers)
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
    saved_timestamp = get_latest_timestamp(SCRAPER_ID, COMPANY_ID)

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

        update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
        return
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


if __name__ == "__main__":
    main()
