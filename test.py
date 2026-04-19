import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = "https://publicaccess.huntingdonshire.gov.uk/online-applications/search.do?action=monthlyList"

session = requests.Session()
response = session.get(url, verify=False)

soup = BeautifulSoup(response.text, "html.parser")

csrf_token = None

# Try hidden input with name containing 'csrf' or '_token'
token_input = soup.find("input", {"name": lambda n: n and ("csrf" in n.lower() or "token" in n.lower())})
if token_input:
    csrf_token = token_input.get("value")

# Fallback: meta tag
if not csrf_token:
    meta = soup.find("meta", {"name": lambda n: n and "csrf" in n.lower()})
    if meta:
        csrf_token = meta.get("content")

print("CSRF Token:", csrf_token)

# Extract org.apache.struts.taglib.html.TOKEN
struts_token_input = soup.find("input", {"name": "org.apache.struts.taglib.html.TOKEN"})
struts_token = struts_token_input.get("value") if struts_token_input else None
print("Struts Token:", struts_token)

# Second request: monthly list results
results_url = "https://publicaccess.huntingdonshire.gov.uk/online-applications/monthlyListResults.do?action=firstPage"

payload = {
    "action": "firstPage",
    "org.apache.struts.taglib.html.TOKEN": struts_token,
    "_csrf": csrf_token,
    "searchCriteria.parish": "",
    "searchCriteria.ward": "",
    "month": "Apr 26",
    "dateType": "DC_Validated",
    "searchType": "Application",
}

results_response = session.post(results_url, data=payload, verify=False)

print("Monthly list results status:", results_response.status_code)

# Fetch 100 results in a single request via pagedSearchResults.do
base_url = "https://publicaccess.huntingdonshire.gov.uk"
paged_url = f"{base_url}/online-applications/pagedSearchResults.do"

paged_payload = {
    "_csrf": csrf_token,
    "searchCriteria.page": 1,
    "action": "page",
    "orderBy": "DateReceived",
    "orderByDirection": "Descending",
    "searchCriteria.resultsPerPage": 100,
}
resp = session.post(paged_url, data=paged_payload, verify=False)

soup2 = BeautifulSoup(resp.text, "html.parser")
all_links = []
for li in soup2.find_all("li", class_="searchresult"):
    a = li.find("a", class_="summaryLink")
    if a and a.get("href"):
        all_links.append(base_url + a["href"])

print(f"Total links collected: {len(all_links)}")
for link in all_links:
    print(link)
