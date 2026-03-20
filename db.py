import os
from supabase import create_client, Client
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Initialize Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Please set SUPABASE_URL and SUPABASE_KEY environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def _utc_now_iso():
    return datetime.utcnow().isoformat()


def _resolve_scraper_id(scraper_ref):
    """
    Resolve scraper ID from either numeric scraper_id or source_name.
    """
    if isinstance(scraper_ref, int):
        return scraper_ref

    if isinstance(scraper_ref, str) and scraper_ref.isdigit():
        return int(scraper_ref)

    if not isinstance(scraper_ref, str):
        raise ValueError("scraper_ref must be scraper_id (int) or source_name (str)")

    result = (
        supabase.table("scrapers")
        .select("id")
        .eq("source_name", scraper_ref)
        .limit(1)
        .execute()
    )

    if not result.data:
        raise ValueError(f"Scraper not found for source_name: {scraper_ref}")

    return int(result.data[0]["id"])


# ----------------------------------------------------------
# Get latest timestamp for a company-scraper pair
# ----------------------------------------------------------
def get_latest_timestamp(scraper_ref, company_id):
    """
    Get the latest timestamp for a specific company + scraper.
    scraper_ref can be either scraper_id (int) or source_name (str).
    Returns None if this is the first run.
    """
    try:
        scraper_id = _resolve_scraper_id(scraper_ref)

        result = (
            supabase.table("company_scrapers")
            .select("latest_timestamp")\
            .eq("company_id", company_id)\
            .eq("scraper_id", scraper_id)\
            .single()\
            .execute()
        )

        return result.data["latest_timestamp"] if result.data else None
    except Exception:
        # If no record exists, this is the first run
        return None


# ----------------------------------------------------------
# Update latest timestamp for a company-scraper pair
# ----------------------------------------------------------
def update_latest_timestamp(scraper_ref, company_id, timestamp):
    """
    Update the latest timestamp for a company + scraper pair.
    scraper_ref can be either scraper_id (int) or source_name (str).
    Creates a new record if it doesn't exist.
    """
    try:
        scraper_id = _resolve_scraper_id(scraper_ref)
        now_iso = _utc_now_iso()

        # Try to update existing record
        result = (
            supabase.table("company_scrapers")
            .update({"latest_timestamp": timestamp, "updated_at": now_iso})
            .eq("company_id", company_id)
            .eq("scraper_id", scraper_id)
            .execute()
        )

        # If no rows affected, insert new record
        if not result.data:
            supabase.table("company_scrapers")\
                .insert({
                    "company_id": company_id,
                    "scraper_id": scraper_id,
                    "latest_timestamp": timestamp,
                    "subscribed_at": now_iso,
                    "updated_at": now_iso,
                    "is_active": True
                })\
                .execute()
    except Exception as e:
        print(f"Error updating timestamp: {e}")
        raise


# ----------------------------------------------------------
# Insert articles into database
# ----------------------------------------------------------
def insert_articles(articles, company_id=None, scraper_id=None):
    """
    Insert articles into global articles table and link them in company_articles.

    Behavior:
    - Global dedupe is by URL in articles.
    - Company-specific access is stored in company_articles.
    - If URL already exists globally but company link is missing, it is created.

    Returns the number of company_articles rows inserted for this call.
    """
    if not articles:
        return 0

    try:
        now_iso = _utc_now_iso()

        normalized = []
        for article in articles:
            this_company_id = article.get("company_id") or company_id
            this_scraper_id = article.get("scraper_id") or scraper_id

            if not this_company_id or not this_scraper_id:
                raise ValueError("Each article requires company_id and scraper_id")

            normalized.append(
                {
                    "company_id": this_company_id,
                    "scraper_id": int(this_scraper_id),
                    "url": article["url"],
                    "date": article.get("date"),
                    "title": article.get("title") or "",
                    "text": article.get("text"),
                    "categories": article.get("categories"),
                    "tags": article.get("tags"),
                    "lastmod": article.get("lastmod"),
                }
            )

        urls = list({a["url"] for a in normalized})

        existing_res = (
            supabase.table("articles")
            .select("id,url")
            .in_("url", urls)
            .execute()
        )
        url_to_id = {row["url"]: row["id"] for row in (existing_res.data or [])}

        missing_articles = []
        for a in normalized:
            if a["url"] not in url_to_id:
                missing_articles.append(
                    {
                        "scraper_id": a["scraper_id"],
                        "url": a["url"],
                        "date": a["date"],
                        "title": a["title"],
                        "text": a["text"],
                        "categories": a["categories"],
                        "tags": a["tags"],
                        "lastmod": a["lastmod"],
                        "created_at": now_iso,
                        "updated_at": now_iso,
                    }
                )

        if missing_articles:
            # ignore_duplicates handles race conditions on articles.url
            supabase.table("articles").upsert(
                missing_articles,
                on_conflict="url",
                ignore_duplicates=True,
            ).execute()

            # Refresh IDs for any newly inserted URLs
            refreshed_res = (
                supabase.table("articles")
                .select("id,url")
                .in_("url", urls)
                .execute()
            )
            url_to_id = {row["url"]: row["id"] for row in (refreshed_res.data or [])}

        company_links = []
        for a in normalized:
            article_id = url_to_id.get(a["url"])
            if not article_id:
                continue

            company_links.append(
                {
                    "company_id": a["company_id"],
                    "article_id": article_id,
                    "scraper_id": a["scraper_id"],
                    "discovered_at": now_iso,
                    "status": "queued",
                }
            )

        if not company_links:
            return 0

        link_result = supabase.table("company_articles").upsert(
            company_links,
            on_conflict="company_id,article_id",
            ignore_duplicates=True,
        ).execute()

        return len(link_result.data) if link_result.data else 0
    except Exception as e:
        print(f"Error inserting articles: {e}")
        raise


# ----------------------------------------------------------
# Check if a company already has access to an article URL
# ----------------------------------------------------------
def article_exists(company_id, url):
    """
    Check if company has a linked article for the provided URL.
    """
    try:
        article_res = (
            supabase.table("articles")
            .select("id")
            .eq("url", url)
            .limit(1)
            .execute()
        )
        if not article_res.data:
            return False

        article_id = article_res.data[0]["id"]
        link_res = (
            supabase.table("company_articles")
            .select("id")
            .eq("company_id", company_id)
            .eq("article_id", article_id)
            .limit(1)
            .execute()
        )
        return bool(link_res.data)
    except Exception as e:
        print(f"Error checking article existence: {e}")
        return False
