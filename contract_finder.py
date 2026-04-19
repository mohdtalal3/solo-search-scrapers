import time
import os
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from db import get_latest_timestamp, update_latest_timestamp, insert_articles

BASE_URL = "https://www.contractsfinder.service.gov.uk"
SEARCH_URL = f"{BASE_URL}/Search/Results"
SOURCE_NAME = "CONTRACT_FINDER"
SCRAPER_ID = 2

# ----------------------------------------------------------
# Company configs — each runs a separate search with its own
# CPV codes, keywords, min value, and company_id.
# ----------------------------------------------------------
DEFAULT_NOTICE_TYPES = ["awarded", "open", "public_notice", "supplychain_notice"]

COMPANY_CONFIGS = [
    {
        "label": "Solo Search (Digital Health / IT)",
        "company_id": os.getenv("SOLO_SEARCH_COMPANY_ID"),
        "keywords": "Integrated Care Board NHS England Trust Digital Software EPR Platform Interoperability Cloud Cyber AI Data",
        "value_low": "250000",
        "notice_types": DEFAULT_NOTICE_TYPES,
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
        "label": "Arden Executive (Defence / Aerospace / Marine / Energy)",
        "company_id": os.getenv("ARDEN_EXEC_COMPANY_ID"),
        "keywords": "defence MOD aerospace aircraft naval marine shipbuilding offshore subsea oil gas energy power plant engineering manufacturing industrial automation framework programme program contract upgrade capability systems integration infrastructure",
        "value_low": "500000",
        "notice_types": DEFAULT_NOTICE_TYPES,
        "cpv_codes": [
            "35300000",
            "35310000",
            "35320000",
            "35600000",
            "35610000",
            "35620000",
            "35630000",
            "35700000",

            "34700000",
            "34710000",
            "34720000",
            "34730000",

            "34500000",
            "34510000",
            "34520000",
            "34530000",
            "34540000",

            "45251000",
            "45251100",
            "45251200",
            "45252000",
            "45262300",

            "71300000",
            "71320000",
            "71330000",

            "42900000"
        ],
    },
    {
        "label": "PLEA (Landscape Architecture)",
        "company_id": os.getenv("PLEA_COMPANY_ID"),
        "keywords": '"landscape architect" "landscape design" "public realm" "biodiversity net gain" "green infrastructure" "planting scheme"',
        "value_low": "",
        "notice_types": ["speculative", "planning", "tender", "awarded", "open", "public_notice", "supplychain_notice"],
        "cpv_codes": [],
    },
]

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
    r = session.get(SEARCH_URL, headers=HEADERS, timeout=200)
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

def submit_search(session: requests.Session, form_token: str, keywords: str, cpv_codes: list, value_low: str, notice_types: list = None) -> str:
    if notice_types is None:
        notice_types = DEFAULT_NOTICE_TYPES

    # Calculate date 2 days ago from today
    two_days_ago = datetime.now() - timedelta(days=2)
    day = two_days_ago.strftime("%d")
    month = two_days_ago.strftime("%m")
    year = two_days_ago.strftime("%Y")

    # 🔴 IMPORTANT: payload as list of tuples (NOT dict)
    data = [("keywords", keywords)]

    for nt in notice_types:
        data.append((nt, "1"))

    data += [
        ("location", "all_locations"),
        ("postcode", ""),
        ("postcode_distance_select", "5 miles"),
        ("postcode_distance", "5"),
        ("value_low", value_low),
        ("value_high", ""),
    ]

    if cpv_codes:
        for cpv in cpv_codes:
            data.append(("cpv_code_selections[]", cpv))

    data += [
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
        ("atm_from[day]", ""),
        ("atm_from[month]", ""),
        ("atm_from[year]", ""),
        ("atm_to[day]", ""),
        ("atm_to[month]", ""),
        ("atm_to[year]", ""),
        ("awarded_from[day]", ""),
        ("awarded_from[month]", ""),
        ("awarded_from[year]", ""),
        ("awarded_to[day]", ""),
        ("awarded_to[month]", ""),
        ("awarded_to[year]", ""),
        ("reload_triggered_by", ""),
        ("form_token", form_token),
        ("adv_search", ""),
    ]
    headers = HEADERS | {
        "origin": BASE_URL,
        "referer": SEARCH_URL,
        "content-type": "application/x-www-form-urlencoded",
    }

    r = session.post(
        SEARCH_URL,
        headers=headers,
        data=data,
        timeout=200
    )
    print()
    r.raise_for_status()
    return r.text


def submit_sort(session: requests.Session, sort_token: str) -> str:
    payload = [
        ("sort_select", " Latest publication date"),
        ("sort", "last_publication:DESC"),
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
        timeout=200
    )

    r.raise_for_status()
    return r.text
def download_xml(session: requests.Session, filename="contracts_finder_results.xml"):
    xml_url = f"{BASE_URL}/Search/GetXmlFile"

    headers = {
        "user-agent": HEADERS["user-agent"],
        "accept": "application/xml,text/xml,*/*",
        "referer": SEARCH_URL,
    }

    r = session.get(xml_url, headers=headers, timeout=200)
    r.raise_for_status()

    if not r.text.strip().startswith("<?xml"):
        raise RuntimeError("Did not receive XML content")

    with open(filename, "w", encoding="utf-8") as f:
        f.write(r.text)

    print(f"📄 XML downloaded: {filename}")
    return r.text


def parse_xml_and_extract_contracts(xml_content):
    """
    Parse XML content and extract contract information dynamically.
    Captures all fields without hardcoding specific field names.
    Returns a list of contract dictionaries with all required fields.
    """
    root = ET.fromstring(xml_content)
    contracts = []
    
    def element_to_dict(element, skip_nil=True):
        """
        Recursively convert XML element to dictionary.
        Captures all fields dynamically.
        """
        result = {}
        
        # Check if element is nil
        if skip_nil and element.get('{http://www.w3.org/2001/XMLSchema-instance}nil') == 'true':
            return None
        
        # If element has no children, return its text
        if len(element) == 0:
            return element.text.strip() if element.text else ""
        
        # Process all child elements
        for child in element:
            tag = child.tag
            value = element_to_dict(child, skip_nil)
            
            # Handle multiple children with same tag (like CPV codes)
            if tag in result:
                # Convert to list if not already
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                if value is not None:
                    result[tag].append(value)
            else:
                if value is not None:
                    result[tag] = value
        
        return result
    
    def dict_to_text(data, indent=0, parent_key=""):
        """
        Convert dictionary to formatted text with proper indentation.
        """
        lines = []
        indent_str = "  " * indent
        
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    lines.append(f"{indent_str}{key}:")
                    lines.extend(dict_to_text(value, indent + 1, key))
                elif isinstance(value, list):
                    lines.append(f"{indent_str}{key}:")
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            lines.append(f"{indent_str}  [{i+1}]:")
                            lines.extend(dict_to_text(item, indent + 2, key))
                        else:
                            lines.append(f"{indent_str}  - {item}")
                else:
                    lines.append(f"{indent_str}{key}: {value}")
        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    lines.append(f"{indent_str}[{i+1}]:")
                    lines.extend(dict_to_text(item, indent + 1, parent_key))
                else:
                    lines.append(f"{indent_str}- {item}")
        else:
            lines.append(f"{indent_str}{data}")
        
        return lines
    
    for full_notice in root.findall('FullNotice'):
        try:
            # Convert entire FullNotice to dictionary
            notice_data = element_to_dict(full_notice)
            
            if not notice_data:
                continue
            
            # Extract key fields for database (with fallbacks)
            notice = notice_data.get('Notice', {})
            
            notice_id = notice.get('Id', '')
            title = notice.get('Title', '')
            published_date = notice.get('PublishedDate', '')
            # Strip Z / timezone offset to get plain ISO datetime
            published_date = published_date[:19] if published_date else ''
            last_update = notice.get('LastNotifiableUpdate', published_date)
            last_update = last_update[:19] if last_update else ''
            
            if not notice_id or not title:
                continue
            
            # Build comprehensive text field with ALL data
            text_parts = []
            
            # Start with title
            text_parts.append(f"TITLE: {title}")
            text_parts.append("")
            text_parts.append("=" * 80)
            text_parts.append("COMPLETE NOTICE DETAILS")
            text_parts.append("=" * 80)
            text_parts.append("")
            
            # Add all notice data dynamically
            text_parts.extend(dict_to_text(notice_data))
            
            # Create contract URL
            contract_url = f"{BASE_URL}/Notice/{notice_id}"
            
            # Create contract dictionary
            contract = {
                "url": contract_url,
                "date": published_date,
                "title": title,
                "text": "\n".join(text_parts),
                "lastmod": last_update or published_date,
                "company_id": None,  # set per-run by run_for_company()
                "scraper_id": SCRAPER_ID
            }
            
            contracts.append(contract)
            
        except Exception as e:
            print(f"Error parsing contract: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    return contracts


def run_for_company(config: dict):
    """Run the full contract-finder flow for a single company config."""
    company_id = config["company_id"]
    label = config["label"]
    keywords = config["keywords"]
    cpv_codes = config["cpv_codes"]
    value_low = config["value_low"]
    notice_types = config.get("notice_types", DEFAULT_NOTICE_TYPES)

    print(f"\n{'='*60}")
    print(f"🏢 Running for: {label}")
    print(f"{'='*60}")

    saved_timestamp = get_latest_timestamp(SCRAPER_ID, company_id)

    session = requests.Session()

    time.sleep(2)
    print("Step 1: Load search page")
    initial_token = get_form_token(session)

    time.sleep(2)
    print("Step 2: Submit search")
    search_html = submit_search(session, initial_token, keywords, cpv_codes, value_low, notice_types)

    if "Something went wrong" in search_html:
        raise RuntimeError("Search failed")

    sort_token = extract_sort_token(search_html)
    print("Step 3: Sort token extracted")

    time.sleep(2)
    print("Step 4: Submit sort")
    sorted_html = submit_sort(session, sort_token)

    if "Something went wrong" in sorted_html:
        raise RuntimeError("Sort failed")

    time.sleep(2)
    print("Step 5: Download XML")
    xml_filename = f"contracts_finder_{company_id[:8]}.xml"
    xml_content = download_xml(session, xml_filename)

    print("Step 6: Parsing XML and extracting contracts")
    contracts = parse_xml_and_extract_contracts(xml_content)

    # Stamp each contract with this company's id
    for c in contracts:
        c["company_id"] = company_id

    if not contracts:
        print("⛔ No contracts found in XML")
        return

    print(f"📊 Found {len(contracts)} contracts")

    contracts.sort(key=lambda x: x["lastmod"], reverse=True)
    newest_timestamp = contracts[0]["lastmod"]

    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT saving any contracts to database.")
        print("Saving latest timestamp:", newest_timestamp)
        update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
        return

    # ----------------------------
    # SUBSEQUENT RUNS — save new contracts
    # ----------------------------
    print("Previously saved timestamp:", saved_timestamp)

    new_contracts = [c for c in contracts if c["lastmod"] > saved_timestamp]

    if not new_contracts:
        print("⛔ No new contracts found.")
        return

    print(f"🆕 Found {len(new_contracts)} new contracts.")

    inserted_count = insert_articles(new_contracts)
    print(f"✅ Inserted {inserted_count} contracts into database")

    update_latest_timestamp(SCRAPER_ID, company_id, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)
    print("✅ DONE")


def main():
    for config in COMPANY_CONFIGS:
        run_for_company(config)
        time.sleep(5)  # brief pause between company runs

if __name__ == "__main__":
    main()
