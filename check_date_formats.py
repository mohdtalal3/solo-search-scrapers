"""
One-off diagnostic: inspect date formats stored per scraper in the articles table.
Fetches a sample of dates for each scraper_id and classifies the format.
"""
import re
from collections import defaultdict

from db import supabase

# Scraper ID -> name map (for readable output)
SCRAPER_NAMES = {
    1: "businesscloud",
    2: "contract_finder",
    3: "digital_health",
    4: "eu_startups",
    5: "find_tender",
    6: "htworld",
    7: "htn_co",
    8: "startups_co",
    9: "themanufacturer",
    10: "ukdefencejournal",
    11: "ukri",
    12: "marineindustrynews",
    13: "energyvoice",
    14: "prnewswire",
    15: "consultancy_eu",
    16: "consultancy_uk",
    17: "erp_today",
    18: "computable_nl",
    19: "capgemini",
    20: "oracle",
    21: "deloitte",
    22: "homes_england",
    23: "bidstats",
    24: "huntingdonshire",
    25: "planning_inspectorate",
    26: "eastcambs",
    27: "greater_cambridge",
    28: "cambridge_news",
}

FORMATS = [
    ("ISO with ms+Z",      r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$"),
    ("ISO with ms",        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+$"),
    ("ISO with Z",         r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"),
    ("ISO datetime",       r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
    ("ISO date only",      r"^\d{4}-\d{2}-\d{2}$"),
    ("dd Mon YYYY",        r"^\d{1,2} \w{3} \d{4}$"),
    ("Mon dd, YYYY",       r"^\w{3,9} \d{1,2}, \d{4}$"),
    ("dd/mm/yyyy",         r"^\d{2}/\d{2}/\d{4}$"),
    ("empty/null",         r"^$"),
]


def classify(date_str: str) -> str:
    if date_str is None:
        return "null"
    s = date_str.strip()
    for label, pattern in FORMATS:
        if re.match(pattern, s):
            return label
    return f"unknown ({s[:40]!r})"


def main():
    print("Fetching distinct scraper IDs from articles table...")
    res = supabase.table("articles").select("scraper_id").execute()
    if not res.data:
        print("No articles found.")
        return

    scraper_ids = sorted({row["scraper_id"] for row in res.data if row["scraper_id"]})
    print(f"Found {len(scraper_ids)} scraper(s): {scraper_ids}\n")

    for sid in scraper_ids:
        name = SCRAPER_NAMES.get(sid, f"scraper_{sid}")

        # Fetch up to 50 sample date values for this scraper
        sample_res = (
            supabase.table("articles")
            .select("date")
            .eq("scraper_id", sid)
            .limit(50)
            .execute()
        )
        dates = [row["date"] for row in (sample_res.data or [])]

        format_counts = defaultdict(int)
        examples = {}
        for d in dates:
            fmt = classify(d)
            format_counts[fmt] += 1
            if fmt not in examples:
                examples[fmt] = d

        print(f"[{sid}] {name}")
        for fmt, count in sorted(format_counts.items()):
            ex = examples[fmt]
            print(f"    {fmt:<25} x{count:<4}  e.g. {ex!r}")
        print()


if __name__ == "__main__":
    main()
