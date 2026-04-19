import os
import time
from datetime import datetime

import requests
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from db import get_recent_article_urls, insert_articles

load_dotenv()

SOURCE_NAME = "HUNTINGDONSHIRE"
SCRAPER_ID = 24
COMPANY_ID = os.getenv("PLEA_COMPANY_ID")
_proxy = os.getenv("SCRAPER_PROXY")
PROXIES = {"http": _proxy, "https": _proxy} if _proxy else None

BASE_URL = "https://publicaccess.huntingdonshire.gov.uk"
SEARCH_PAGE_URL = f"{BASE_URL}/online-applications/search.do?action=monthlyList"
MONTHLY_RESULTS_URL = f"{BASE_URL}/online-applications/monthlyListResults.do?action=firstPage"
PAGED_RESULTS_URL = f"{BASE_URL}/online-applications/pagedSearchResults.do"
DETAILS_URL = f"{BASE_URL}/online-applications/applicationDetails.do"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
}


def url_slug(url: str) -> str:
    """Extract keyVal from a planning portal URL as a dedup key."""
    for part in url.split("&"):
        if part.startswith("keyVal="):
            return part.split("=", 1)[1]
    return url.rstrip("/").rsplit("/", 1)[-1]


def current_month_label() -> str:
    """Return month label in the format the site expects, e.g. 'Apr 26'."""
    now = datetime.now()
    return now.strftime("%b") + " " + now.strftime("%y")


def make_session() -> requests.Session:
    s = requests.Session()
    return s


def get_tokens(html: str) -> tuple[str, str]:
    """Extract _csrf and org.apache.struts.taglib.html.TOKEN from page HTML."""
    soup = BeautifulSoup(html, "html.parser")
    # _csrf token
    csrf_tag = soup.find("input", {"name": lambda n: n and ("csrf" in n.lower() or "token" in n.lower())})
    csrf = csrf_tag.get("value", "") if csrf_tag else ""
    if not csrf:
        meta = soup.find("meta", {"name": lambda n: n and "csrf" in n.lower()})
        csrf = meta.get("content", "") if meta else ""
    # struts token
    struts_tag = soup.find("input", {"name": "org.apache.struts.taglib.html.TOKEN"})
    struts = struts_tag.get("value", "") if struts_tag else ""
    return csrf, struts


def init_search(session: requests.Session, date_type: str = "DC_Validated") -> str:
    """GET the search form to establish session cookie + tokens, then POST the monthly list search."""
    r = session.get(SEARCH_PAGE_URL, headers=HEADERS, timeout=30, verify=False)
    r.raise_for_status()
    csrf, struts = get_tokens(r.text)
    time.sleep(1)

    form_data = {
        "action": "firstPage",
        "org.apache.struts.taglib.html.TOKEN": struts,
        "_csrf": csrf,
        "searchCriteria.parish": "",
        "searchCriteria.ward": "",
        "month": current_month_label(),
        "dateType": date_type,
        "searchType": "Application",
    }
    r2 = session.post(
        MONTHLY_RESULTS_URL,
        data=form_data,
        headers=HEADERS | {"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
        verify=False,
    )
    r2.raise_for_status()
    # Re-extract csrf from response in case it rotated
    csrf2, _ = get_tokens(r2.text)
    return r2.text, csrf2 or csrf


def fetch_page(session: requests.Session, csrf: str, page: int) -> str:
    """POST to pagedSearchResults.do for the given page number with 100 results per page."""
    r = session.post(
        PAGED_RESULTS_URL,
        data={
            "_csrf": csrf,
            "searchCriteria.page": page,
            "action": "page",
            "orderBy": "DateReceived",
            "orderByDirection": "Descending",
            "searchCriteria.resultsPerPage": 50,
        },
        headers=HEADERS | {"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
        verify=False,
    )
    r.raise_for_status()
    return r.text


def parse_results(html: str) -> list[tuple[str, str, str]]:
    """
    Parse search results page.
    Returns list of (full_summary_url, key_val, description).
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for li in soup.select("ul#searchresults li.searchresult"):
        a = li.select_one("a.summaryLink")
        if not a:
            continue
        href = a.get("href", "")
        if not href:
            continue
        full_url = BASE_URL + href if href.startswith("/") else href
        # Extract keyVal
        key_val = ""
        for part in href.split("&"):
            if "keyVal=" in part:
                key_val = part.split("keyVal=")[-1]
                break
        desc_el = a.select_one("div.summaryLinkTextClamp")
        description = desc_el.get_text(strip=True) if desc_el else a.get_text(strip=True)
        if key_val:
            results.append((full_url, key_val, description))
    return results


def scrape_print_preview(key_val: str, max_retries: int = 3):
    """
    Fetch the print preview page for a planning application.
    Returns (title, date, body_text).
    """
    print_url = f"{DETAILS_URL}?activeTab=printPreview&keyVal={key_val}"
    for attempt in range(max_retries):
        try:
            time.sleep(1)
            r = requests.get(print_url, headers=HEADERS, proxies=PROXIES, timeout=30, verify=False)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            container = soup.select_one("div#popupContainer")
            if not container:
                return None, None, None

            # Remove empty/whitespace-only rows and noisy elements
            for el in container.select("script, style, img"):
                el.decompose()

            # Title = Proposal field from the summary table
            title = ""
            for tr in container.select("table#simpleDetailsTable tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if th and td and "Proposal" in th.get_text():
                    title = td.get_text(strip=True)
                    break
            if not title:
                h2 = container.select_one("h2")
                title = h2.get_text(strip=True) if h2 else key_val

            # Date = Application Received from summary table
            date = ""
            for tr in container.select("table#simpleDetailsTable tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if th and td and "Application Received" in th.get_text() and "Date" not in th.get_text():
                    raw = td.get_text(strip=True)
                    try:
                        # e.g. "Mon 13 Apr 2026" → "2026-04-13T00:00:00"
                        date = datetime.strptime(raw, "%a %d %b %Y").strftime("%Y-%m-%dT%H:%M:%S")
                    except ValueError:
                        date = raw
                    break

            # Body = all table rows as key: value lines
            lines = []
            for h2 in container.select("h2"):
                section_title = h2.get_text(strip=True)
                lines.append(f"\n=== {section_title} ===")
                table = h2.find_next("table")
                if table:
                    for tr in table.select("tr"):
                        th = tr.select_one("th")
                        td = tr.select_one("td")
                        if th and td:
                            k = th.get_text(strip=True)
                            v = td.get_text(strip=True)
                            if v:
                                lines.append(f"{k}: {v}")

            body = "\n".join(lines).strip()
            return title, date, body

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{max_retries} for {key_val}: {e}")
                time.sleep(2)
            else:
                print(f"  ❌ Failed to scrape {key_val}: {e}")
                return None, None, None


def main():
    print("🔍 Fetching Huntingdonshire planning applications...")

    known_urls = get_recent_article_urls(SCRAPER_ID, limit=500)
    print(f"🗄️  {len(known_urls)} known URLs loaded from DB.")
    seen_slugs = {url_slug(u) for u in known_urls}

    session = make_session()

    all_items = []

    for date_type in ("DC_Validated", "DC_Decided"):
        print(f"  Step: Initialising session + submitting search ({date_type})...")
        time.sleep(1)
        _, csrf = init_search(session, date_type=date_type)

        print(f"  Fetching page 1 of results ({date_type})...")
        time.sleep(1)
        first_page_html = fetch_page(session, csrf, page=1)

        items = parse_results(first_page_html)
        print(f"  📄 {date_type}: {len(items)} application(s) found.")
        all_items.extend(items)

    new_items = []
    for full_url, key_val, description in all_items:
        if full_url in known_urls or key_val in seen_slugs:
            print(f"  ⏭️  Skipping (already in DB): {key_val}")
            continue
        new_items.append((full_url, key_val, description))
        seen_slugs.add(key_val)

    if not new_items:
        print("\n⛔ No new applications found.")
        return

    print(f"  🆕 {len(new_items)} new application(s) to scrape.")

    articles = []
    for full_url, key_val, fallback_title in new_items:
        print(f"  Scraping: {key_val}")
        title, date, body = scrape_print_preview(key_val)
        if title is None:
            continue
        articles.append({
            "url": full_url,
            "date": date,
            "title": title,
            "text": body,
            "company_id": COMPANY_ID,
            "scraper_id": SCRAPER_ID,
        })
        print(f"  ✅ {title[:60]}...")

    if not articles:
        print("\n⛔ No applications scraped successfully.")
        return

    print(f"\n🆕 Found {len(articles)} new application(s) in total.")
    inserted_count = insert_articles(articles)
    print(f"✅ Inserted {inserted_count} applications into database")


if __name__ == "__main__":
    main()
