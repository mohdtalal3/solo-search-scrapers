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


# ----------------------------------------------------------
# Get latest timestamp for a source
# ----------------------------------------------------------
def get_latest_timestamp(source_name):
    """
    Get the latest timestamp stored for a specific source.
    Returns None if this is the first run.
    """
    try:
        result = supabase.table("scraper_metadata")\
            .select("latest_timestamp")\
            .eq("source_name", source_name)\
            .single()\
            .execute()
        
        return result.data["latest_timestamp"] if result.data else None
    except Exception as e:
        # If no record exists, this is the first run
        return None


# ----------------------------------------------------------
# Update latest timestamp for a source
# ----------------------------------------------------------
def update_latest_timestamp(source_name, timestamp):
    """
    Update the latest timestamp for a source.
    Creates a new record if it doesn't exist.
    """
    try:
        # Try to update existing record
        result = supabase.table("scraper_metadata")\
            .update({"latest_timestamp": timestamp, "updated_at": datetime.utcnow().isoformat()})\
            .eq("source_name", source_name)\
            .execute()
        
        # If no rows affected, insert new record
        if not result.data:
            supabase.table("scraper_metadata")\
                .insert({
                    "source_name": source_name,
                    "latest_timestamp": timestamp,
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                })\
                .execute()
    except Exception as e:
        print(f"Error updating timestamp: {e}")
        raise


# ----------------------------------------------------------
# Insert articles into database
# ----------------------------------------------------------
def insert_articles(articles):
    """
    Insert multiple articles into the database using upsert.
    On conflict (duplicate URL), updates the existing record.
    Returns the number of articles successfully inserted/updated.
    """
    if not articles:
        return 0
    
    try:
        # Add/update timestamps for each article
        current_time = datetime.utcnow().isoformat()
        for article in articles:
            if "created_at" not in article:
                article["created_at"] = current_time
            article["updated_at"] = current_time
        
        # Use upsert: insert or update on conflict with url
        result = supabase.table("articles").upsert(articles, on_conflict="url",ignore_duplicates=True ).execute()
        return len(result.data) if result.data else 0
    except Exception as e:
        print(f"Error upserting articles: {e}")
        raise


# ----------------------------------------------------------
# Check if article already exists (by URL)
# ----------------------------------------------------------
def article_exists(url):
    """
    Check if an article with the given URL already exists.
    """
    try:
        result = supabase.table("articles")\
            .select("url")\
            .eq("url", url)\
            .execute()
        
        return len(result.data) > 0 if result.data else False
    except Exception as e:
        print(f"Error checking article existence: {e}")
        return False
