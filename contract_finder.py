import time
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from db import get_latest_timestamp, update_latest_timestamp, insert_articles

BASE_URL = "https://www.contractsfinder.service.gov.uk"
SEARCH_URL = f"{BASE_URL}/Search/Results"
SOURCE_NAME = "CONTRACT_FINDER"

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
    # ("sort_select", " Latest publication date"),
    # ("sort", "last_publication:DESC"),
    ("keywords", "Integrated Care Board NHS England Trust Digital Software EPR Platform Interoperability Cloud Cyber AI Data"),
    ("awarded", "1"),
    ("open", "1"),
    ("public_notice", "1"),
    ("supplychain_notice", "1"),
    ("location", "all_locations"),
    ("postcode", ""),
    ("postcode_distance_select", "5 miles"),
    ("postcode_distance", "5"),
    ("value_low", "250000"),
    ("value_high", ""),
    ("cpv_code_selections[]", "72000000"),
    ("cpv_code_selections[]", "72200000"),
    ("cpv_code_selections[]", "72500000"),
    ("cpv_code_selections[]", "72600000"),
    ("cpv_code_selections[]", "51600000"),
    ("cpv_code_selections[]", "48000000"),
    ("cpv_code_selections[]", "48180000"),
    ("cpv_code_selections[]", "72300000"),
    ("cpv_code_selections[]", "75123000"),
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
    ("form_token", f"{form_token}"),
    ("adv_search", "")
]
    print(data)
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
        timeout=30
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

    r = session.get(xml_url, headers=headers, timeout=30)
    r.raise_for_status()

    if not r.text.strip().startswith("<?xml"):
        raise RuntimeError("Did not receive XML content")

    with open(filename, "w", encoding="utf-8") as f:
        f.write(r.text)

    print(f"📄 XML downloaded: {filename}")
    return r.text


def parse_xml_and_extract_contracts(xml_content):
    """
    Parse XML content and extract contract information.
    Returns a list of contract dictionaries with all required fields.
    """
    root = ET.fromstring(xml_content)
    
    # Define namespace
    ns = {'': 'http://www.w3.org/2001/XMLSchema-instance'}
    
    contracts = []
    
    for full_notice in root.findall('FullNotice'):
        try:
            notice = full_notice.find('Notice')
            if notice is None:
                continue
            
            # Extract basic information
            notice_id = notice.findtext('Id', '').strip()
            title = notice.findtext('Title', '').strip()
            description = notice.findtext('Description', '').strip()
            published_date = notice.findtext('PublishedDate', '').strip()
            last_update = notice.findtext('LastNotifiableUpdate', '').strip()
            identifier = notice.findtext('Identifier', '').strip()
            
            # Extract contact details
            contact = notice.find('ContactDetails')
            contact_name = contact.findtext('Name', '') if contact is not None else ''
            contact_email = contact.findtext('Email', '') if contact is not None else ''
            contact_address = ''
            if contact is not None:
                addr1 = contact.findtext('Address1', '')
                town = contact.findtext('Town', '')
                postcode = contact.findtext('Postcode', '')
                country = contact.findtext('Country', '')
                contact_address = f"{addr1}, {town}, {postcode}, {country}".strip(', ')
            
            # Extract values
            value_low = notice.findtext('ValueLow', '')
            value_high = notice.findtext('ValueHigh', '')
            
            # Extract dates
            start_date = notice.findtext('Start', '')
            end_date = notice.findtext('End', '')
            deadline_date = notice.findtext('DeadlineDate', '')
            
            # Extract CPV codes
            cpv_codes_elem = notice.find('CpvCodes')
            cpv_codes = []
            if cpv_codes_elem is not None:
                cpv_codes = [code.text for code in cpv_codes_elem.findall('string') if code.text]
            
            # Extract status and type
            status = notice.findtext('Status', '')
            contract_type = notice.findtext('Type', '')
            ojeu_type = notice.findtext('OjeuContractType', '')
            procedure_type = notice.findtext('ProcedureType', '')
            
            # Extract organization
            org_name = notice.findtext('OrganisationName', '')
            
            # Extract location
            location_elem = notice.find('Location')
            latitude = location_elem.findtext('Lat', '') if location_elem is not None else ''
            longitude = location_elem.findtext('Lon', '') if location_elem is not None else ''
            region = notice.findtext('Region', '')
            postcode_loc = notice.findtext('Postcode', '')
            
            # Extract SME and VCO suitability
            suitable_sme = notice.findtext('IsSuitableForSme', '')
            suitable_vco = notice.findtext('IsSuitableForVco', '')
            
            # Extract award details
            awards_info = []
            awards = full_notice.find('Awards')
            if awards is not None:
                for award in awards.findall('AwardDetail'):
                    award_value = award.findtext('Value', '')
                    supplier_name = award.findtext('SupplierName', '')
                    supplier_address = award.findtext('SupplierAddress', '')
                    awarded_date = award.findtext('AwardedDate', '')
                    award_start = award.findtext('StartDate', '')
                    award_end = award.findtext('EndDate', '')
                    awarded_to_sme = award.findtext('AwardedToSME', '')
                    
                    awards_info.append({
                        'supplier_name': supplier_name,
                        'supplier_address': supplier_address,
                        'value': award_value,
                        'awarded_date': awarded_date,
                        'start_date': award_start,
                        'end_date': award_end,
                        'awarded_to_sme': awarded_to_sme
                    })
            
            # Build comprehensive text field
            text_parts = []
            
            # Title
            text_parts.append(f"TITLE: {title}")
            text_parts.append("")
            
            # Description
            text_parts.append(f"DESCRIPTION:\n{description}")
            text_parts.append("")
            
            # Contract Details
            text_parts.append("CONTRACT DETAILS:")
            text_parts.append(f"Identifier: {identifier}")
            text_parts.append(f"Notice ID: {notice_id}")
            text_parts.append(f"Status: {status}")
            text_parts.append(f"Type: {contract_type}")
            text_parts.append(f"OJEU Contract Type: {ojeu_type}")
            if procedure_type:
                text_parts.append(f"Procedure Type: {procedure_type}")
            text_parts.append("")
            
            # Financial Information
            text_parts.append("FINANCIAL INFORMATION:")
            if value_low:
                text_parts.append(f"Value (Low): £{value_low}")
            if value_high:
                text_parts.append(f"Value (High): £{value_high}")
            text_parts.append("")
            
            # Dates
            text_parts.append("IMPORTANT DATES:")
            text_parts.append(f"Published: {published_date}")
            if deadline_date:
                text_parts.append(f"Deadline: {deadline_date}")
            if start_date:
                text_parts.append(f"Contract Start: {start_date}")
            if end_date:
                text_parts.append(f"Contract End: {end_date}")
            text_parts.append("")
            
            # Organization
            text_parts.append("ORGANIZATION:")
            text_parts.append(f"Name: {org_name}")
            text_parts.append("")
            
            # Contact Information
            text_parts.append("CONTACT INFORMATION:")
            text_parts.append(f"Name: {contact_name}")
            text_parts.append(f"Email: {contact_email}")
            text_parts.append(f"Address: {contact_address}")
            text_parts.append("")
            
            # Location
            text_parts.append("LOCATION:")
            if region:
                text_parts.append(f"Region: {region}")
            if postcode_loc:
                text_parts.append(f"Postcode: {postcode_loc}")
            if latitude and longitude:
                text_parts.append(f"Coordinates: {latitude}, {longitude}")
            text_parts.append("")
            
            # CPV Codes
            if cpv_codes:
                text_parts.append("CPV CODES:")
                for code in cpv_codes:
                    text_parts.append(f"- {code}")
                text_parts.append("")
            
            # Suitability
            text_parts.append("SUITABILITY:")
            text_parts.append(f"Suitable for SME: {suitable_sme}")
            text_parts.append(f"Suitable for VCO: {suitable_vco}")
            text_parts.append("")
            
            # Award Information
            if awards_info:
                text_parts.append("AWARD INFORMATION:")
                for i, award in enumerate(awards_info, 1):
                    text_parts.append(f"\nAward #{i}:")
                    text_parts.append(f"  Supplier: {award['supplier_name']}")
                    text_parts.append(f"  Address: {award['supplier_address']}")
                    text_parts.append(f"  Value: £{award['value']}")
                    text_parts.append(f"  Awarded Date: {award['awarded_date']}")
                    text_parts.append(f"  Start Date: {award['start_date']}")
                    text_parts.append(f"  End Date: {award['end_date']}")
                    text_parts.append(f"  Awarded to SME: {award['awarded_to_sme']}")
                text_parts.append("")
            
            # Create contract URL
            contract_url = f"{BASE_URL}/Notice/{notice_id}"
            
            # Create contract dictionary
            contract = {
                "source": SOURCE_NAME,
                "group_name": "1",
                "url": contract_url,
                "date": published_date,
                "title": title,
                "text": "\n".join(text_parts),
                "lastmod": last_update or published_date
            }
            
            contracts.append(contract)
            
        except Exception as e:
            print(f"Error parsing contract: {e}")
            continue
    
    return contracts


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

    # with open("01_results_unsorted.html", "w", encoding="utf-8") as f:
    #     f.write(search_html)

    # Extract sort token from results page
    sort_token = extract_sort_token(search_html)
    print("Step 3: Sort token extracted")

    time.sleep(2)
    print("Step 4: Submit sort")
    sorted_html = submit_sort(session, sort_token)

    if "Something went wrong" in sorted_html:
        raise RuntimeError("Sort failed")

    # with open("02_results_sorted.html", "w", encoding="utf-8") as f:
    #     f.write(sorted_html)

    # Download XML
    time.sleep(2)
    print("Step 5: Download XML")
    xml_content = download_xml(session, "contracts_finder_sorted.xml")
    
    # Parse XML and extract contracts
    print("Step 6: Parsing XML and extracting contracts")
    contracts = parse_xml_and_extract_contracts(xml_content)
    
    if not contracts:
        print("⛔ No contracts found in XML")
        return
    
    print(f"📊 Found {len(contracts)} contracts")
    
    # Sort by lastmod (newest first)
    contracts.sort(key=lambda x: x["lastmod"], reverse=True)
    newest_timestamp = contracts[0]["lastmod"]
    
    # ----------------------------
    # FIRST RUN — NO SCRAPING
    # ----------------------------
    if saved_timestamp is None:
        print("🟢 First run detected — NOT saving any contracts to database.")
        print("Saving latest timestamp:", newest_timestamp)
        update_latest_timestamp(SOURCE_NAME, newest_timestamp)
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
    
    # Insert contracts into database
    inserted_count = insert_articles(new_contracts)
    print(f"✅ Inserted {inserted_count} contracts into database")
    
    # Update timestamp
    update_latest_timestamp(SOURCE_NAME, newest_timestamp)
    print("🕒 New latest timestamp saved:", newest_timestamp)
    print("✅ ALL DONE")




if __name__ == "__main__":
    main()
