# Scraper Patterns Reference

Quick reference for all patterns used across this scraping codebase. Use this when building a new scraper.

---

## 1. Deduplication Strategies

### A. Timestamp-Based (most common)
**When to use:** Source has reliable `lastmod` / `date_gmt` / `published_date` field. Best for sitemaps and WordPress APIs.

**DB functions:** `get_latest_timestamp`, `update_latest_timestamp`

**Flow:**
```
1. Load saved_timestamp from DB
2. Fetch articles sorted newest-first
3. newest_timestamp = articles[0]["lastmod"]
4. If saved_timestamp is None → FIRST RUN: save timestamp only, no insert, return
5. Filter: new_articles = [a for a in articles if a["lastmod"] > saved_timestamp]
6. Scrape + insert new articles
7. update_latest_timestamp(newest_timestamp)
```

**Used in:** `digital_health.py`, `htn_co.py`, `startups_co.py`, `ukri.py`, `themanufacturer.py`, `thegrocer.py`, `marketingweek.py`, `htworld.py`, `erp_today.py`, `businesscloud.py`, `energyvoice.py`

---

### B. URL Slug Deduplication
**When to use:** Source has no reliable timestamp. Listing page scraper. JS-rendered pages.

**DB functions:** `get_recent_article_urls`

**Flow:**
```
1. known_urls = get_recent_article_urls(SCRAPER_ID, limit=200)  → set of URLs from DB
2. seen_slugs = {url_slug(u) for u in known_urls}
3. Fetch all listing URLs
4. Deduplicate listing results (same URL appearing twice)
5. For each URL:
   - if url in known_urls → skip (exact URL match)
   - if url_slug(url) in seen_slugs → skip (slug match, handles URL param variants)
   - else → add to new_items
6. Scrape + insert new_items
```

**`url_slug()` helper:**
```python
from urllib.parse import urlparse
def url_slug(url):
    return urlparse(url).path.rstrip("/").split("/")[-1]
```

**Used in:** `thedrum.py`, `prolificnorth.py`, `businesswire.py`, `companies_house.py`, `capgemini.py`, `deloitte.py`, `oracle.py`, `cambridge_news.py`

---

## 2. Source Types

### A. WordPress REST API
**Endpoint:** `/wp-json/wp/v2/posts`
**Dedup:** Timestamp (date_gmt)
**Key params:** `per_page=100`, `page=N`, `_fields=id,link,date_gmt,title,content`

```python
API_URL = "https://example.com/wp-json/wp/v2/posts"
params = {
    "per_page": 100,
    "page": page_num,
    "_fields": "id,link,date_gmt,title,content",
    "orderby": "date",
    "order": "desc",
}
resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
posts = resp.json()
# Each post: post["date_gmt"], post["title"]["rendered"], post["content"]["rendered"], post["link"]
```

**Used in:** `marketingweek.py`, `htworld.py`, `eu_startups.py`, `businesscloud.py`, `htn_co.py`

---

### B. WordPress Sitemap (sitemap_index.xml → post-sitemap.xml)
**Dedup:** Timestamp (`lastmod`)
**Pattern:** Fetch index → find highest-numbered `post-sitemap.xml` → parse URLs + lastmods

```python
MAIN_SITEMAP = "https://example.com/sitemap_index.xml"
# Index has: <sitemap><loc>post-sitemap.xml</loc></sitemap>
# Post sitemap has: <url><loc>...</loc><lastmod>2026-...</lastmod></url>
```

**Used in:** `digital_health.py`, `htn_co.py`, `startups_co.py`, `themanufacturer.py`, `ukri.py`

---

### C. Google News Sitemap
**Dedup:** Timestamp (`lastmod`)
**Pattern:** Direct XML with `news:` namespace or `lastmod` per entry

```python
# URL pattern: https://example.com/sitemap-news.xml?page=1
# Parse with BeautifulSoup xml parser
for url_tag in soup.find_all("url"):
    loc = url_tag.find("loc").get_text(strip=True)
    pub_date = url_tag.find("news:publication_date").get_text(strip=True)[:19]
    title = url_tag.find("news:title").get_text(strip=True)
```

**Used in:** `prnewswire.py`

---

### D. Custom/Yearly Sitemap (Google schema 0.84)
**Dedup:** Timestamp (`lastmod`)
**Pattern:** Yearly URL, Google's own sitemap namespace

```python
SITEMAP_NS = {"sm": "http://www.google.com/schemas/sitemap/0.84"}
year = datetime.now().year
sitemap_url = f"https://example.com/googlesitemap.aspx?year={year}"
# Parse with xml.etree.ElementTree
root = ET.fromstring(xml_content)
for url_el in root.findall("sm:url", SITEMAP_NS):
    loc = url_el.find("sm:loc", SITEMAP_NS).text
    lastmod = url_el.find("sm:lastmod", SITEMAP_NS).text
```

**Used in:** `thegrocer.py`

---

### E. HTML Listing Scraper
**Dedup:** URL slug
**Pattern:** Scrape paginated listing page → collect article URLs → scrape each

```python
for page in range(1, MAX_PAGES + 1):
    url = f"{LISTING_URL}&paged={page}"
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")
    # Extract article links from listing
```

**Used in:** `thedrum.py`, `prolificnorth.py`, `businesswire.py`, `consultancy_eu.py`, `consultancy_uk.py`

---

### F. Search/API with Form Submission (Contract Finder / Find Tender)
**Dedup:** Timestamp (`lastmod`)
**Pattern:** GET form page → extract `form_token` → POST search → sort → download XML

```python
# Step 1: Get token
token = get_form_token(session)  # BeautifulSoup parse of form page
# Step 2: POST search with payload (keywords, notice types, value_low, etc.)
search_html = submit_search(session, token, ...)
# Step 3: Sort + download XML
xml = download_xml(session)
# Step 4: Parse XML for contracts
```

**Used in:** `contract_finder.py`, `find_tender.py`

---

### G. Elasticsearch/Internal JSON API
**Dedup:** URL slug
**Pattern:** POST to internal search endpoint, parse JSON response

**Used in:** `deloitte.py`, `capgemini.py`, `oracle.py`

---

### H. Mantis/Custom News API
**Used in:** `cambridge_news.py`

---

### I. Planning/Government Portals (pagination scraper)
**Dedup:** URL slug
**Pattern:** Keyword search → paginate results → scrape individual pages

**Used in:** `huntingdonshire.py`, `eastcambs.py`, `greater_cambridge.py`, `bidstats.py`, `planning_inspectorate.py`

---

## 3. HTTP Fetch Methods

### A. Plain `requests`
**Use when:** Site is simple, no bot detection.
```python
import requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
resp = requests.get(url, headers=HEADERS, timeout=30)
```

---

### B. `curl_cffi` (Chrome Impersonation)
**Use when:** Site has basic bot detection / TLS fingerprinting. JS-rendered listing pages.
```python
from curl_cffi import requests

def get_proxies():
    proxy = os.getenv("SCRAPER_PROXY")
    return {"http": proxy, "https": proxy} if proxy else None

resp = requests.get(
    url,
    impersonate="chrome131",
    proxies=get_proxies(),
    timeout=30,
)
```

**Used in:** `prolificnorth.py`, `digital_health.py` (articles), `businesswire.py`

---

### C. Scrappey (Full Browser / Captcha Solving)
**Use when:** Cloudflare, heavy JS rendering, captcha.

**Standard payload:**
```python
SCRAPPEY_API_URL = "https://publisher.scrappey.com/api/v1"
SCRAPPEY_PROXY_COUNTRY = "UnitedKingdom"

payload = {
    "cmd": "request.get",
    "url": target_url,
    "premiumProxy": True,
    "proxyCountry": SCRAPPEY_PROXY_COUNTRY,
    "retries": 1,
    "automaticallySolveCaptcha": True,
    "browserActions": [
        {"type": "wait_for_load_state", "waitForLoadState": "networkidle"},
        {"type": "wait", "wait": 1500, "when": "after_captcha"}
    ]
}

api_key = os.getenv("SCRAPPEY_API_KEY")
resp = requests.post(f"{SCRAPPEY_API_URL}?key={api_key}", json=payload, timeout=90)
data = resp.json()
solution = data.get("solution", {})

# Check for errors
if data.get("data") == "error" or not solution.get("verified", False):
    raise RuntimeError(data.get("error", "Unknown Scrappey error"))

html = solution.get("response", "")  # The page HTML
```

**For WordPress JSON API through Scrappey:**
```python
# The JSON is embedded in solution["response"] as a string
import json
raw = solution.get("response", "[]")
posts = json.loads(raw)
```

**Used in:** `eu_startups.py`, `themanufacturer.py`, `digital_health.py` (sitemaps), `htworld.py` (uses SeleniumBase)

---

### D. SeleniumBase (Undetected Chrome)
**Use when:** Heavy JS site, need full browser. Slower than Scrappey.
```python
from seleniumbase import SB
with SB(uc=True, headless=True) as sb:
    sb.open(url)
    html = sb.get_page_source()
```

**Used in:** `htworld.py`

---

## 4. Proxy Setup

```python
# .env
SCRAPER_PROXY=http://user:password@host:port

# In scraper
def get_proxies():
    proxy = os.getenv("SCRAPER_PROXY")
    return {"http": proxy, "https": proxy} if proxy else None

proxies = get_proxies()
requests.get(url, proxies=proxies, ...)
```

Proxy: `http://18cda7346607998ed56e__cr.gb:9d269735267c3bd8@gw.dataimpulse.com:823`

---

## 5. Multi-Company Scrapers

For scrapers that serve multiple companies with different keywords/configs:

```python
COMPANY_CONFIGS = [
    {"label": "Company A", "company_id": os.getenv("COMPANY_A_ID"), "keywords": "..."},
    {"label": "Company B", "company_id": os.getenv("COMPANY_B_ID"), "keywords": "..."},
]

def main():
    for config in COMPANY_CONFIGS:
        run_for_company(config)

def run_for_company(config):
    company_id = config["company_id"]
    # Check subscription before doing any work
    if not is_subscription_active(SCRAPER_ID, company_id):
        print(f"⏭️  Skipping {config['label']} — subscription is inactive")
        return
    # ... scrape and insert
```

**Used in:** `contract_finder.py`, `find_tender.py`, `prnewswire.py`, `consultancy_eu.py`, `consultancy_uk.py`, `companies_house.py`

---

## 6. DB Functions (`db.py`)

| Function | Args | Returns | Use case |
|----------|------|---------|----------|
| `get_latest_timestamp(scraper_id, company_id)` | int, str | str or None | Timestamp dedup — get last saved time |
| `update_latest_timestamp(scraper_id, company_id, ts)` | int, str, str | — | Save newest timestamp after run |
| `get_recent_article_urls(scraper_id, limit=32)` | int, int | set of str | URL slug dedup — load known URLs |
| `insert_articles(articles)` | list of dicts | int (inserted count) | Insert scraped articles |
| `load_active_subscriptions()` | — | — | Load all company-scraper is_active statuses. Call once at start of scheduler run |
| `is_subscription_active(scraper_id, company_id)` | int, str | bool | Check if company subscription is active before scraping |

**Article dict shape:**
```python
{
    "url": "https://...",          # required, unique key
    "date": "2026-05-01T12:00:00", # ISO datetime string
    "title": "Article Title",      # required
    "text": "Body text...",
    "lastmod": "2026-05-01T12:00:00",
    "company_id": "uuid-...",
    "scraper_id": 3,
    # optional:
    "categories": ["News", "Tech"],
    "tags": ["AI", "NHS"],
}
```

---

## 7. Subscription Active Check (is_active)

`main.py` calls `db.load_active_subscriptions()` once per scheduler run. This caches all `company_scrapers.is_active` values.

**Single-company scrapers:** Checked in `run_scraper()` via `scraper_module` param. Module must have `SCRAPER_ID` and `COMPANY_ID` at module level.

**Multi-company scrapers:** Each scraper checks `is_subscription_active()` internally, per company, before processing.

```python
# In main.py — single company
run_scraper("The Drum", thedrum.main, thedrum)        # module passed → auto-checked
run_scraper("Contract Finder", contract_finder.main)  # no module → checked internally

# In multi-company scraper
if not is_subscription_active(SCRAPER_ID, company_id):
    print(f"⏭️  Skipping {label} — subscription is inactive")
    return  # or continue
```

---

## 8. First Run Behaviour

All scrapers follow this pattern on first run (no saved timestamp):
- **Do NOT scrape or insert any articles**
- Just save the newest available timestamp
- On next run, only articles newer than that timestamp are scraped

This prevents flooding the DB with historical articles on first deploy.

```python
if saved_timestamp is None:
    print("🟢 First run detected — NOT scraping any articles.")
    update_latest_timestamp(SCRAPER_ID, COMPANY_ID, newest_timestamp)
    return
```

---

## 9. Scraper ID → File Map

| ID | File | Company | Type |
|----|------|---------|------|
| 1 | `businesscloud.py` | Solo Search | WP API + timestamp |
| 2 | `contract_finder.py` | All | Form POST + XML |
| 3 | `digital_health.py` | Solo Search | WP Sitemap + Scrappey |
| 4 | `eu_startups.py` | Solo Search | WP API + Scrappey |
| 5 | `find_tender.py` | Multiple | Form POST + XML |
| 6 | `htworld.py` | Solo Search | WP API + SeleniumBase |
| 7 | `htn_co.py` | Solo Search | WP Sitemap + timestamp |
| 8 | `startups_co.py` | Solo Search | WP Sitemap + timestamp |
| 9 | `themanufacturer.py` | Arden Exec | WP Sitemap + Scrappey |
| 10 | `ukdefencejournal.py` | Arden Exec | WP Sitemap |
| 11 | `ukri.py` | Solo Search | WP Sitemap + timestamp |
| 12 | `marineindustrynews.py` | Arden Exec | HTML listing |
| 13 | `energyvoice.py` | Arden Exec | WP API + timestamp |
| 14 | `prnewswire.py` | Multi | News sitemap + timestamp |
| 15 | `consultancy_eu.py` | ERP Recruit | HTML listing + URL slug |
| 16 | `consultancy_uk.py` | ERP Recruit | HTML listing + URL slug |
| 17 | `erp_today.py` | ERP Recruit | WP API + timestamp |
| 18 | `computable_nl.py` | ERP Recruit | WP API |
| 19 | `capgemini.py` | ERP Recruit | JSON API + URL slug |
| 20 | `oracle.py` | ERP Recruit | JSON API + URL slug |
| 21 | `deloitte.py` | ERP Recruit | Elasticsearch + URL slug |
| 22 | `homes_england.py` | PLEA | Gov search |
| 23 | `bidstats.py` | PLEA | HTML listing |
| 24 | `huntingdonshire.py` | PLEA | Planning portal |
| 25 | `planning_inspectorate.py` | PLEA | Gov appeals |
| 26 | `eastcambs.py` | PLEA | Planning portal |
| 27 | `greater_cambridge.py` | PLEA | Planning portal |
| 28 | `cambridge_news.py` | PLEA | Mantis API |
| 29 | `companies_house.py` | Multi | Gov search + URL slug |
| 30 | `thedrum.py` | Headliners | HTML listing + URL slug |
| 31 | `businesswire.py` | Headliners | HTML listing + curl_cffi |
| 32 | `marketingweek.py` | Headliners | WP API + timestamp |
| 33 | `prolificnorth.py` | Headliners | HTML listing + curl_cffi |
| 34 | `thegrocer.py` | Headliners | Yearly sitemap + timestamp |

---

## 10. Company IDs

| Company | Env Var | UUID |
|---------|---------|------|
| Solo Search | `SOLO_SEARCH_COMPANY_ID` | `234f37eb-1147-43fb-89c1-9812e0824e1f` |
| Arden Executive | `ARDEN_EXEC_COMPANY_ID` | `c5d7d2eb-189d-49dc-89e1-2af375d0b3ce` |
| ERP Recruit | `ERP_RECRUIT_COMPANY_ID` | `81ef7ff7-c548-42d8-8b22-b339b26d08ac` |
| PLEA | `PLEA_COMPANY_ID` | *(check .env)* |
| Headliners | `HEADLINERS_COMPANY_ID` | `16adda91-be84-461a-b6e2-dc81e76cc2c6` |
