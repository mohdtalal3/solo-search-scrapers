import time
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from db import get_latest_timestamp, update_latest_timestamp, insert_articles
import re

BASE_URL = "https://www.find-tender.service.gov.uk"
SEARCH_URL = f"{BASE_URL}/Search/Results"
SOURCE_NAME = "FIND_TENDER"
SCRAPER_ID = 5

# ----------------------------------------------------------
# Company configs — each runs a separate search with its own
# CPV codes, keywords, min value, and company_id.
# ----------------------------------------------------------
COMPANY_CONFIGS = [
    {
        "label": "Solo Search (Digital Health / IT)",
        "company_id": os.getenv("SOLO_SEARCH_COMPANY_ID"),
        "keywords": "Integrated Care Board NHS England Trust Digital Software EPR Platform Interoperability Cloud Cyber AI Data",
        "value_low": "250000",
        "cpv_codes": [
            "72000000",
            "72200000",
            "72500000",
            "72600000",
            "51600000",
            "48000000",
            "48180000",
            "72300000",
            "75123000",
        ],
    },
    {
        "label": "ERP Recruit (Oracle / ERP)",
        "company_id": os.getenv("ERP_RECRUIT_COMPANY_ID"),
        "keywords": "Oracle ERP",
        "value_low": "50000",
        "cpv_codes": [
        ],
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
}


def build_session() -> requests.Session:
    s = requests.Session()
    proxy_url = os.getenv("FIND_TENDER_PROXY")
    if proxy_url:
        s.proxies = {"http": proxy_url, "https": proxy_url}
        print(f"  🔒 Proxy active: {proxy_url.split('@')[-1]}")
    return s

def get_form_token(session: requests.Session) -> str:
    r = session.get(SEARCH_URL, headers=HEADERS, timeout=120)
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

def submit_search(session: requests.Session, form_token: str, keywords: str, cpv_codes: list, value_low: str) -> str:
    two_days_ago = datetime.now() - timedelta(days=2)
    day = two_days_ago.strftime("%d")
    month = two_days_ago.strftime("%m")
    year = two_days_ago.strftime("%Y")

    data = [
        ("keywords", keywords),
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
    ]

    if cpv_codes:
        for cpv in cpv_codes:
            data.append(("cpv_code_selections[]", cpv))

    data += [
        ("minimum_value", value_low),
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
        ("form_token", form_token),
        ("adv_search", ""),
    ]

    headers = HEADERS | {
        "origin": BASE_URL,
        "referer": SEARCH_URL,
        "content-type": "application/x-www-form-urlencoded",
    }

    r = session.post(SEARCH_URL, headers=headers, data=data, timeout=120)
    r.raise_for_status()
    return r.text
def submit_sort(session: requests.Session, sort_token: str) -> str:
    payload = [
        ("sort_select", "Published (newest)"),
        ("sort", "unix_published_date:DESC"),
        ("form_token", sort_token),
    ]

    headers = HEADERS | {
        "origin": BASE_URL,
        "referer": SEARCH_URL,
        "content-type": "application/x-www-form-urlencoded",
    }

    r = session.post(
        SEARCH_URL,
        headers=headers,
        data=payload,
        timeout=120
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


SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"


def scrape_notice_details(session, notice_url):
    """
    Fetch notice detail page via Scrappey (bypasses bot protection).
    Falls back to plain requests if Scrappey key is not set.
    """
    full_url = f"{BASE_URL}{notice_url}" if notice_url.startswith("/") else notice_url

    try:
        scrappey_key = os.getenv("SCRAPPEY_API_KEY")
        if scrappey_key:
            payload = {
                "cmd": "request.get",
                "requestType": "request",
                "retries":2,
                #"cmd": "request.get",
                "url": full_url,
                "premiumProxy": True,
                "proxyCountry": "UnitedKingdom",
            }
            resp = requests.post(
                f"{SCRAPPEY_API_URL}?key={scrappey_key}",
                json=payload,
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
            html = data.get("solution", {}).get("response", "")
        else:
            r = session.get(full_url, headers=HEADERS, timeout=120  )
            r.raise_for_status()
            html = r.text

        soup = BeautifulSoup(html, "html.parser")
        content_div = soup.select_one(".notice-view.govuk-main-wrapper.app-main-class")
        if not content_div:
            return ""
        return content_div.get_text("\n", strip=True)

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
        resp = session.get(page_url, headers=HEADERS, timeout=120)
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


def run_for_company(config: dict):
    company_id = config["company_id"]
    label = config["label"]
    keywords = config["keywords"]
    cpv_codes = config["cpv_codes"]
    value_low = config["value_low"]

    print(f"\n{'='*60}")
    print(f"🏢 Running for: {label}")
    print(f"{'='*60}")

    saved_timestamp = get_latest_timestamp(SCRAPER_ID, company_id)

    session = build_session()

    time.sleep(2)
    print("Step 1: Load search page")
    initial_token = get_form_token(session)

    time.sleep(2)
    print("Step 2: Submit search")
    search_html = submit_search(session, initial_token, keywords, cpv_codes, value_low)

    if "Something went wrong" in search_html:
        raise RuntimeError("Search failed")

    sort_token = extract_sort_token(search_html)
    print("Step 3: Sort token extracted")

    time.sleep(2)
    print("Step 4: Submit sort")
    sorted_html = submit_sort(session, sort_token)

    if "Something went wrong" in sorted_html:
        raise RuntimeError("Sort failed")

    soup = BeautifulSoup(sorted_html, "html.parser")
    last_page = get_last_page(soup)
    print(f"Step 5: Last page detected: {last_page}")

    all_notices = []
    for page in range(1, last_page + 1):
        page_url = f"{SEARCH_URL}?page={page}"
        print(f"\nFetching page {page}/{last_page}: {page_url}")
        notices = extract_notices_from_page(session, page_url)
        all_notices.extend(notices)
        time.sleep(1)

    print(f"\n📊 Found {len(all_notices)} total notices across {last_page} pages")

    if not all_notices:
        print("⛔ No notices found")
        return

    all_notices.sort(key=lambda x: x["publication_date"], reverse=True)
    newest_timestamp = all_notices[0]["publication_date"]

    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT saving any notices to database.")
        print("Saving latest timestamp:", newest_timestamp)
        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
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

    contracts = []
    for i, notice in enumerate(new_notices, 1):
        print(f"Scraping notice {i}/{len(new_notices)}: {notice['title']}")

        full_text = scrape_notice_details(session, notice["url"])

        text_parts = [
            f"TITLE: {notice['title']}",
            "",
            "FULL NOTICE DETAILS:",
            full_text,
        ]

        full_url = f"{BASE_URL}{notice['url']}" if notice["url"].startswith("/") else notice["url"]

        contracts.append({
            "url": full_url,
            "date": notice["publication_date"],
            "title": notice["title"],
            "text": "\n".join(text_parts),
            "lastmod": notice["publication_date"],
            "company_id": company_id,
            "scraper_id": SCRAPER_ID,
        })
        time.sleep(1)

    inserted_count = insert_articles(contracts)
    print(f"✅ Inserted {inserted_count} notices into database")

    update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)
    print("✅ DONE")


def main():
    for config in COMPANY_CONFIGS:
        run_for_company(config)
        time.sleep(5)


if __name__ == "__main__":
    main()