from curl_cffi import requests
from bs4 import BeautifulSoup
import time
from db import get_latest_timestamp, update_latest_timestamp, insert_articles

API_URL = "https://www.eu-startups.com/wp-json/wp/v2/posts"
SOURCE_NAME = "EU_STARTUPS"


def clean_html_content(html_content):
    """Convert HTML content to clean text"""
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Remove unwanted elements
    for tag in soup.select("script, style, iframe"):
        tag.decompose()
    
    # Get text and clean up whitespace
    text = soup.get_text(" ", strip=True)
    text = " ".join(text.split())
    return text


def fetch_posts_with_retry(session, params, max_retries=3):
    """Fetch posts with retry logic"""
    for attempt in range(max_retries):
        try:
            time.sleep(2)  # Sleep 2 seconds before request
            r = session.get(API_URL, params=params, timeout=30)
            
            if r.status_code == 400:
                return None  # No more pages
            
            r.raise_for_status()
            return r.json()
            
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Retry {attempt + 1}/{max_retries}: {str(e)}")
                time.sleep(2)
            else:
                print(f"❌ Failed after {max_retries} attempts: {str(e)}")
                return None


def main():
    saved_timestamp = get_latest_timestamp(SOURCE_NAME)
    
    session = requests.Session(impersonate="chrome")
    
    all_articles = []
    newest_timestamp = None
    
    print("🔍 Fetching articles from EU-Startups API...")
    
    # Fetch first page only
    params = {
        "categories": 1282,  # Funding category
        "per_page": 100,
        "page": 1,
        "orderby": "date",
        "order": "desc"
    }
    
    posts = fetch_posts_with_retry(session, params)
    
    if posts is None or not posts:
        print("⛔ No articles found.")
        return
    
    for post in posts:
        timestamp = post["date_gmt"]
        
        # Set newest timestamp from first article
        if newest_timestamp is None:
            newest_timestamp = timestamp
        
        # Stop if we've reached articles older than saved timestamp
        if saved_timestamp and timestamp <= saved_timestamp:
            break
        
        # Extract and clean content
        title = post["title"]["rendered"]
        html_content = post["content"]["rendered"]
        text = clean_html_content(html_content)
        
        article = {
            "source": SOURCE_NAME,
            "group_name": "2",
            "url": post["link"],
            "date": timestamp,
            "title": title,
            "text": text,
            "lastmod": timestamp
        }
        
        all_articles.append(article)
        print(f"Fetched: {title[:60]}...")
    
    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT saving any articles.")
        if newest_timestamp:
            print("Saving latest timestamp:", newest_timestamp)
            update_latest_timestamp(SOURCE_NAME, newest_timestamp)
        return
    
    # ----------------------------
    # SUBSEQUENT RUNS — save new
    # ----------------------------
    print("Previously saved timestamp:", saved_timestamp)
    
    if not all_articles:
        print("⛔ No new articles found.")
        return
    
    print(f"🆕 Found {len(all_articles)} new articles.")
    
    # Insert articles into database
    if all_articles:
        inserted_count = insert_articles(all_articles)
        print(f"✅ Inserted {inserted_count} articles into database")
    
    # Update timestamp
    if newest_timestamp:
        update_latest_timestamp(SOURCE_NAME, newest_timestamp)
        print("🕒 New latest timestamp saved:", newest_timestamp)


if __name__ == "__main__":
    main()
