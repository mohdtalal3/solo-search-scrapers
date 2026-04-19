"""
One-off migration: normalise all article dates in the DB to YYYY-MM-DDTHH:MM:SS.
Reads articles in batches, converts each date, and patches only rows that changed.
"""
import re
from datetime import datetime

from db import supabase

BATCH_SIZE = 1000  # rows per Supabase page

# Dutch month abbreviations used by Capgemini NL pages
_DUTCH_MONTHS = {
    "jan.": "Jan", "feb.": "Feb", "mrt.": "Mar", "apr.": "Apr",
    "mei":  "May", "jun.": "Jun", "jul.": "Jul", "aug.": "Aug",
    "sep.": "Sep", "okt.": "Oct", "nov.": "Nov", "dec.": "Dec",
}

# Ordered list of (format_string, strptime_pattern) to try
_PARSE_FORMATS = [
    "%d %B %Y %I:%M %p",    # December 10, 2025 12:29 PM  (htn_co after normalisation)
    "%B %d, %Y %I:%M %p",   # December 10, 2025 12:29 PM  (alternate)
    "%d %B %Y",              # 10 December 2025 / 2 February 2026
    "%d %b %Y",              # 25 Mar 2026
    "%b %d, %Y",             # Mar 16, 2026  (capgemini EN)
    "%Y-%m-%d",              # 2026-04-16  (date-only ISO)
]


def _to_iso(raw: str) -> str:
    """
    Convert any known date string to YYYY-MM-DDTHH:MM:SS.
    Returns the original string unchanged if it already matches or can't be parsed.
    """
    if not raw:
        return raw

    s = raw.strip()

    # Already correct ISO datetime (with or without trailing Z / offset)
    if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", s):
        return s[:19]

    # Normalise Dutch month abbreviations (capgemini NL)
    first_word = s.split()[0].lower() if s else ""
    if first_word in _DUTCH_MONTHS:
        s = _DUTCH_MONTHS[first_word] + s[len(first_word):]

    # Normalise am/pm for 12-hour formats
    s_ampm = s.replace(" am", " AM").replace(" pm", " PM")

    for fmt in _PARSE_FORMATS:
        src = s_ampm if "%p" in fmt else s
        try:
            return datetime.strptime(src, fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue

    return raw  # leave untouched if nothing matched


def main():
    print("Starting date migration...\n")

    offset = 0
    total_checked = 0
    total_updated = 0

    while True:
        res = (
            supabase.table("articles")
            .select("id,date")
            .range(offset, offset + BATCH_SIZE - 1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            break

        updates = []
        for row in rows:
            original = row["date"]
            fixed = _to_iso(original) if original else original
            if fixed != original:
                updates.append({"id": row["id"], "date": fixed})

        total_checked += len(rows)

        if updates:
            # Update each row individually (update, not upsert, to avoid not-null violations)
            for row in updates:
                supabase.table("articles").update({"date": row["date"]}).eq("id", row["id"]).execute()
                
            total_updated += len(updates)
                
            print(f"  Rows {offset}–{offset + len(rows) - 1}: {len(updates)} date(s) fixed.")
        else:
            print(f"  Rows {offset}–{offset + len(rows) - 1}: all OK.")

        offset += BATCH_SIZE

        if len(rows) < BATCH_SIZE:
            break

    print(f"\nDone. Checked {total_checked} articles, updated {total_updated} dates.")


if __name__ == "__main__":
    main()
