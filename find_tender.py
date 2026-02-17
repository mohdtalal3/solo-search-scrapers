import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from db import get_latest_timestamp, update_latest_timestamp, insert_articles
import re

BASE_URL = "https://www.find-tender.service.gov.uk"
SEARCH_URL = f"{BASE_URL}/Search/Results"
SOURCE_NAME = "FIND_TENDER"

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
}

def get_form_token(session: requests.Session) -> str:
    r = session.get(SEARCH_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    token = soup.find("input", {"name": "form_token"})
    if not token:
        raise RuntimeError("form_token not found")

    return token["value"]
def extract_sort_token(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    token = soup.find("input", {"name": "form_token"})
    if not token:
        raise RuntimeError("Sort form_token not found")

    return token["value"]

def submit_search(session: requests.Session, form_token: str) -> str:
    # Calculate date 2 days ago from today
    two_days_ago = datetime.now() - timedelta(days=2)
    day = two_days_ago.strftime("%d")
    month = two_days_ago.strftime("%m")
    year = two_days_ago.strftime("%Y")
    
    # 🔴 IMPORTANT: payload as list of tuples (NOT dict)
    data = [
    ("keywords", "Integrated Care Board NHS England Trust Digital Software EPR Platform Interoperability Cloud Cyber AI Data"),
    ("stage[5]", "1"),
    ("form_type[28]", "1"),
    ("form_type[30]", "1"),
    ("form_type[31]", "1"),
    ("form_type[29]", "1"),
    ("form_type[32]", "1"),
    ("form_type[33]", "1"),
    ("form_type[34]", "1"),
    ("form_type[37]", "1"),
    ("form_type[38]", "1"),
    ("form_type[39]", "1"),
    ("form_type[35]", "1"),
    ("form_type[41]", "1"),
    ("form_type[42]", "1"),
    ("form_type[43]", "1"),
    ("form_type[1]", "1"),
    ("form_type[2]", "1"),
    ("form_type[3]", "1"),
    ("form_type[4]", "1"),
    ("form_type[5]", "1"),
    ("form_type[6]", "1"),
    ("form_type[7]", "1"),
    ("form_type[8]", "1"),
    ("form_type[12]", "1"),
    ("form_type[13]", "1"),
    ("form_type[14]", "1"),
    ("form_type[15]", "1"),
    ("form_type[16]", "1"),
    ("form_type[17]", "1"),
    ("form_type[18]", "1"),
    ("form_type[19]", "1"),
    ("form_type[20]", "1"),
    ("form_type[21]", "1"),
    ("form_type[22]", "1"),
    ("form_type[23]", "1"),
    ("form_type[24]", "1"),
    ("form_type[25]", "1"),
    ("form_type[26]", "1"),
    ("form_type[27]", "1"),
    ("cpv_code_selections[]", "72000000"),
    ("cpv_code_selections[]", "72200000"),
    ("cpv_code_selections[]", "72500000"),
    ("cpv_code_selections[]", "72600000"),
    ("cpv_code_selections[]", "51600000"),
    ("cpv_code_selections[]", "48000000"),
    ("cpv_code_selections[]", "48180000"),
    ("cpv_code_selections[]", "72300000"),
    ("cpv_code_selections[]", "75123000"),
    ("minimum_value", "250000"),
    ("maximum_value", ""),
    ("published_from[day]", day),
    ("published_from[month]", month),
    ("published_from[year]", year),
    ("published_to[day]", ""),
    ("published_to[month]", ""),
    ("published_to[year]", ""),
    ("closed_from[day]", ""),
    ("closed_from[month]", ""),
    ("closed_from[year]", ""),
    ("closed_to[day]", ""),
    ("closed_to[month]", ""),
    ("closed_to[year]", ""),
    ("reload_triggered_by", ""),
    ("form_token", f"{form_token}"),
    ("adv_search", "")
]
   # print(data)
    headers = HEADERS | {
        "origin": BASE_URL,
        "referer": SEARCH_URL,
        "content-type": "application/x-www-form-urlencoded",
    }

    r = session.post(
        SEARCH_URL,
        headers=headers,
        data=data,
        timeout=30
    )

    r.raise_for_status()
    return r.text
def submit_sort(session: requests.Session, sort_token: str) -> str:
    payload = [
        ("sort_select", "Published (newest)"),
        ("sort", "unix_published_date:DESC"),
        ("form_token", sort_token),
    ]

    headers = {
        "user-agent": HEADERS["user-agent"],
        "accept": HEADERS["accept"],
        "accept-language": HEADERS["accept-language"],
        "origin": BASE_URL,
        "referer": SEARCH_URL,
        "content-type": "application/x-www-form-urlencoded",
    }

    r = session.post(
        SEARCH_URL,
        headers=headers,
        data=payload,
        timeout=30
    )

    r.raise_for_status()
    return r.text


def parse_publication_date(date_text):
    """
    Parse publication date like "12 December 2025, 5:49pm"
    Returns ISO format datetime string for database storage
    """
    try:
        # Remove "Publication date" prefix if present
        date_text = date_text.replace("Publication date", "").strip()
        
        # Parse the date
        dt = datetime.strptime(date_text, "%d %B %Y, %I:%M%p")
        return dt.isoformat() + "Z"
    except Exception as e:
        print(f"Error parsing date '{date_text}': {e}")
        return ""


def get_last_page(soup):
    """
    Find the last page number by reading pagination text.
    Works because Find-a-Tender always shows numeric page links.
    """
    pages = []

    for a in soup.select(".gadget-footer-paginate li.standard-paginate a"):
        text = a.get_text(strip=True)
        if text.isdigit():
            pages.append(int(text))

    return max(pages) if pages else 1


def scrape_notice_details(session, notice_url):
    """
    Scrape the full details of a notice from its detail page.
    Returns the complete text content.
    """
    try:
        full_url = f"{BASE_URL}{notice_url}" if notice_url.startswith("/") else notice_url
        
        resp = session.get(full_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Get the main content
        content_div = soup.select_one(".notice-view.govuk-main-wrapper.app-main-class")
        
        if not content_div:
            return ""
        
        # Extract all text content
        text = content_div.get_text("\n", strip=True)
        
        return text
        
    except Exception as e:
        print(f"Error scraping notice {notice_url}: {e}")
        return ""


def extract_notices_from_page(session, page_url):
    """
    Extract all notice links and metadata from a search results page.
    Returns list of notice dictionaries with title, date, and url.
    """
    notices = []
    
    try:
        resp = session.get(page_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for result in soup.select(".search-result"):
            # Title + URL
            link_el = result.select_one("h2 a")
            title = link_el.get_text(strip=True) if link_el else None
            url = link_el["href"] if link_el else None

            # Publication date
            publication_date = None
            for entry in result.select(".search-result-entry"):
                dt = entry.select_one("dt")
                dd = entry.select_one("dd")
                if dt and "Publication date" in dt.get_text():
                    raw_date = dd.get_text(strip=True)
                    publication_date = parse_publication_date(raw_date)
                    break

            if title and url and publication_date:
                notices.append({
                    "title": title,
                    "publication_date": publication_date,
                    "url": url
                })
                
    except Exception as e:
        print(f"Error extracting notices from page: {e}")
    
    return notices


def main():
    saved_timestamp = get_latest_timestamp(SOURCE_NAME)

    session = requests.Session()

    time.sleep(2)
    print("Step 1: Load search page")
    initial_token = get_form_token(session)

    time.sleep(2)
    print("Step 2: Submit search")
    search_html = submit_search(session, initial_token)

    if "Something went wrong" in search_html:
        raise RuntimeError("Search failed")

    # Extract sort token from results page
    sort_token = extract_sort_token(search_html)
    print("Step 3: Sort token extracted")

    time.sleep(2)
    print("Step 4: Submit sort")
    sorted_html = submit_sort(session, sort_token)

    if "Something went wrong" in sorted_html:
        raise RuntimeError("Sort failed")

    # # Save sorted results to file for debugging
    # with open("find_tender_results.html", "w", encoding="utf-8") as f:
    #     f.write(sorted_html)
    
    # Parse the sorted results to get the search results URL
    soup = BeautifulSoup(sorted_html, "html.parser")
    
    # Get last page number
    last_page = get_last_page(soup)
    print(f"Step 5: Last page detected: {last_page}")
    
    # Extract all notices from all pages
    all_notices = []
    
    for page in range(1, last_page + 1):
        page_url = f"{SEARCH_URL}?page={page}"
        print(f"\nFetching page {page}/{last_page}: {page_url}")
        
        notices = extract_notices_from_page(session, page_url)
        all_notices.extend(notices)
        
        time.sleep(1)  # Be nice to the server
    
    print(f"\n📊 Found {len(all_notices)} total notices across {last_page} pages")
    
    if not all_notices:
        print("⛔ No notices found")
        return
    
    # Sort by publication date (newest first)
    all_notices.sort(key=lambda x: x["publication_date"], reverse=True)
    newest_timestamp = all_notices[0]["publication_date"]
    
    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT saving any notices to database.")
        print("Saving latest timestamp:", newest_timestamp)
        update_latest_timestamp(SOURCE_NAME, newest_timestamp)
        return
    
    # ----------------------------
    # SUBSEQUENT RUNS — save new notices
    # ----------------------------
    print("Previously saved timestamp:", saved_timestamp)
    
    new_notices = [n for n in all_notices if n["publication_date"] > saved_timestamp]
    
    if not new_notices:
        print("⛔ No new notices found.")
        return
    
    print(f"🆕 Found {len(new_notices)} new notices.")
    
    # Scrape full details for each new notice
    contracts = []
    for i, notice in enumerate(new_notices, 1):
        print(f"Scraping notice {i}/{len(new_notices)}: {notice['title']}")
        
        # Get full notice details
        full_text = scrape_notice_details(session, notice["url"])
        
        # Build comprehensive text field with title first
        text_parts = []
        text_parts.append(f"TITLE: {notice['title']}")
        text_parts.append("")
        text_parts.append("FULL NOTICE DETAILS:")
        text_parts.append(full_text)
        
        # Create full URL
        full_url = f"{BASE_URL}{notice['url']}" if notice['url'].startswith("/") else notice['url']
        
        contract = {
            "url": full_url,
            "date": notice["publication_date"],
            "title": notice["title"],
            "text": "\n".join(text_parts),
            "lastmod": notice["publication_date"],
            "company_id": "234f37eb-1147-43fb-89c1-9812e0824e1f",
            "scraper_id": 6
        }
        
        contracts.append(contract)
        time.sleep(1)  # Be nice to the server
    
    # Insert contracts into database
    inserted_count = insert_articles(contracts)
    print(f"✅ Inserted {inserted_count} notices into database")
    
    # Update timestamp
    update_latest_timestamp(SOURCE_NAME, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)
    print("✅ ALL DONE")




if __name__ == "__main__":
    main()
