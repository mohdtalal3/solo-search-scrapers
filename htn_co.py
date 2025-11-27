import requests
from bs4 import BeautifulSoup
import json
import os

MAIN_SITEMAP = "https://htn.co.uk/wp-sitemap.xml"
DATA_FILE = "articles.json"

headers = {"User-Agent": "Mozilla/5.0"}


# ----------------------------------------------------------
# Load existing combined data
# ----------------------------------------------------------
def load_data():
    if not os.path.exists(DATA_FILE):
        return {"latest_timestamp": None, "articles": []}
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# ----------------------------------------------------------
# Save combined data
# ----------------------------------------------------------
def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


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

    # Remove irrelevant sections
    for sel in [".crp_related", ".adv_content", ".addthis_tool"]:
        for tag in content_div.select(sel):
            tag.decompose()

    paragraphs = [p.get_text(strip=True) for p in content_div.find_all("p")]
    text = "\n\n".join(paragraphs)

    return {
        "url": url,
        "date": date_tag.get_text(strip=True) if date_tag else "",
        "title": title_tag.get_text(strip=True) if title_tag else "",
        "text": text,
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
    data = load_data()
    saved_timestamp = data["latest_timestamp"]

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

        data["latest_timestamp"] = newest_timestamp
        save_data(data)
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

    for article in new_articles:
        print("Scraping:", article["url"])
        scraped = scrape_article(article["url"])
        if scraped:
            scraped["lastmod"] = article["lastmod"]
            data["articles"].append(scraped)

    # Update timestamp
    data["latest_timestamp"] = newest_timestamp
    save_data(data)

    print("✅ Updated JSON file:", DATA_FILE)
    print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
