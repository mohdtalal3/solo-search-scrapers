import requests
from bs4 import BeautifulSoup
import json
import os
from db import get_latest_timestamp, update_latest_timestamp, insert_articles

MAIN_SITEMAP = "https://htn.co.uk/wp-sitemap.xml"
SOURCE_NAME = "HTN_CO"

headers = {"User-Agent": "Mozilla/5.0"}


# ----------------------------------------------------------
# Scrape a single article
# ----------------------------------------------------------
def scrape_article(url):
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.text, "html.parser")

    title_tag = soup.find("h1", class_="entry-title")
    date_tag = soup.find("time", class_="entry-date")
    content_div = soup.find("div", class_="entry-content")

    if not content_div:
        return None

    # -----------------------------
    # CATEGORIES
    # -----------------------------
    cat_links = soup.find("span", class_="cat-links")
    categories = []
    if cat_links:
        categories = [a.get_text(strip=True) for a in cat_links.find_all("a")]

    # Remove irrelevant sections
    for sel in [".crp_related", ".adv_content", ".addthis_tool"]:
        for tag in content_div.select(sel):
            tag.decompose()

    paragraphs = [p.get_text(strip=True) for p in content_div.find_all("p")]
    text = "\n\n".join(paragraphs)

    return {
        "source": SOURCE_NAME,
        "group_name": "1",
        "url": url,
        "date": date_tag.get_text(strip=True) if date_tag else "",
        "title": title_tag.get_text(strip=True) if title_tag else "",
        "categories": categories,
        "text": text,
        "company_id": "234f37eb-1147-43fb-89c1-9812e0824e1f",
    }


# ----------------------------------------------------------
# Get the latest "post" sitemap
# ----------------------------------------------------------
def get_latest_post_sitemap():
    resp = requests.get(MAIN_SITEMAP, headers=headers)
    soup = BeautifulSoup(resp.text, "xml")

    links = []
    for loc in soup.find_all("loc"):
        link = loc.text.strip()
        if "wp-sitemap-posts-post-" in link:
            links.append(link)

    if not links:
        raise Exception("No post sitemap links found.")

    links.sort(key=lambda x: int(x.split("-post-")[1].split(".")[0]))
    return links[-1]


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
